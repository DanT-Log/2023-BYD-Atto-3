"""Read-only freshness test for BYD's cloud charging-status read.

Question: can plug-in and charging be detected by reading ONLY BYD's cloud
(/control/smartCharge/homePage), without asking the car anything?

Every normal poll makes two requests that ask the car to report in (full
status and GPS) plus this one cloud read. Everything the tracker uses to
detect a charge (charging state, plug state, power, battery %) comes from
the cloud read. If the cloud's copy stays fresh on its own, idle checks
could use just that and stop asking the car, which is the likely cost of
polling.

This script makes NO ask to the car. After one login it calls only
get_charging_status, once a minute, and records for each read:
  * the state (charging_state, connect_state, soc, power)
  * the cloud's own "last data update" timestamp and how old it is, which
    shows directly whether BYD's data refreshes by itself or sits stale
    until the car is asked
  * how long the call took

Run it while plugging in and unplugging by hand, then compare when the
cloud's copy flipped against when it actually happened.

What it can NOT tell us: whether the read itself wakes the car. That can
only be judged over days, from battery loss.

Privacy: the repo and its Actions logs are public. The public log shows
only the state values above and ages. Full responses go to the private
mqtt_test_events table (RLS on, no policies, service key only).
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from importlib import metadata
from typing import Any
from zoneinfo import ZoneInfo

import requests
from pybyd import BydClient, BydConfig

TEST_MINUTES = float(os.environ.get("TEST_MINUTES") or 30)
READ_EVERY_S = float(os.environ.get("READ_EVERY_S") or 60)
RUN_LABEL = os.environ.get("RUN_LABEL") or "read-test"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
TZ = ZoneInfo("Australia/Sydney")
START = time.monotonic()
_pool = ThreadPoolExecutor(max_workers=2)


def say(msg: str) -> None:
    t = int(time.monotonic() - START)
    print(f"[T+{t // 60:02d}:{t % 60:02d} | {datetime.now(TZ):%H:%M:%S}] {msg}", flush=True)


def _post_row(row: dict[str, Any]) -> None:
    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/mqtt_test_events",
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
            },
            json=row,
            timeout=15,
        )
        if not resp.ok:  # never print the body: it would echo the row back
            print(f"warning: could not store row (HTTP {resp.status_code})", file=sys.stderr, flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"warning: could not store row ({type(exc).__name__})", file=sys.stderr, flush=True)


def store(event: str, payload: Any) -> None:
    _pool.submit(_post_row, {"run_label": RUN_LABEL, "event": event, "topic": None, "payload": payload})


def meta(kind: str, **info: Any) -> None:
    store("_meta", {"kind": kind, **info})
    say(f"-- {kind} {' '.join(f'{k}={v}' for k, v in info.items())}".rstrip())


def fmt_age(age_s: float | None) -> str:
    if age_s is None:
        return "unknown age"
    if age_s < 0:
        return "in the future?"
    if age_s < 120:
        return f"{age_s:.0f}s"
    if age_s < 7200:
        return f"{age_s / 60:.0f} min"
    return f"{age_s / 3600:.1f} h"


def snapshot(status: Any, latency_s: float) -> dict[str, Any]:
    raw = status.raw if isinstance(getattr(status, "raw", None), dict) else {}
    updated = status.update_time
    age_s = None
    if isinstance(updated, datetime):
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age_s = (datetime.now(timezone.utc) - updated).total_seconds()
    return {
        "soc": status.soc,
        "charging_state": status.charging_state,
        # same field the poller uses for "is a cable plugged in"
        "connect_state": raw.get("connectState", status.connect_state),
        "charge_power_kw": raw.get("chargePower"),
        "update_time": updated.isoformat() if isinstance(updated, datetime) else None,
        "age_s": age_s,
        "latency_s": round(latency_s, 2),
        "raw": raw,
    }


async def main() -> None:
    config = BydConfig.from_env()
    async with BydClient(config) as client:
        try:
            version = metadata.version("pybyd")
        except Exception:  # noqa: BLE001
            version = "unknown"
        meta("script_started", pybyd=version, minutes=TEST_MINUTES, every_s=READ_EVERY_S, label=RUN_LABEL)

        vehicles = await client.get_vehicles()
        vin = vehicles[0].vin  # never printed: public log

        reads = errors = 0
        last: dict[str, Any] | None = None
        changes: list[str] = []
        distinct_updates: set[str] = set()
        deadline = START + TEST_MINUTES * 60
        next_read = time.monotonic()

        while time.monotonic() < deadline:
            t0 = time.monotonic()
            try:
                # The ONLY request this script ever makes after login. No
                # get_vehicle_realtime and no get_gps_info: those ask the car.
                status = await client.get_charging_status(vin)
                snap = snapshot(status, time.monotonic() - t0)
                reads += 1
                store("charging_read", snap)

                refreshed = bool(last is not None and snap["update_time"] != last["update_time"])
                if snap["update_time"]:
                    distinct_updates.add(snap["update_time"])
                power = snap["charge_power_kw"] if snap["charge_power_kw"] is not None else "-"
                say(
                    f"read #{reads} | soc={snap['soc']} charging_state={snap['charging_state']} "
                    f"connect_state={snap['connect_state']} power={power} | "
                    f"cloud data {fmt_age(snap['age_s'])} old{' (REFRESHED since last read)' if refreshed else ''} | "
                    f"{snap['latency_s']}s"
                )

                if last is not None and (snap["charging_state"], snap["connect_state"]) != (
                    last["charging_state"], last["connect_state"]
                ):
                    desc = (
                        f"charging_state {last['charging_state']}->{snap['charging_state']}, "
                        f"connect_state {last['connect_state']}->{snap['connect_state']}"
                    )
                    changes.append(f"{datetime.now(TZ):%H:%M:%S} {desc}")
                    meta("state_changed", change=desc, cloud_age=fmt_age(snap["age_s"]))
                last = snap
            except Exception as exc:  # noqa: BLE001
                errors += 1
                say(f"read failed: {type(exc).__name__}: {str(exc)[:160]}")
                store("charging_read_error", {"error": f"{type(exc).__name__}: {str(exc)[:300]}"})

            next_read += READ_EVERY_S
            await asyncio.sleep(max(0.0, next_read - time.monotonic()))

        meta("finished", reads=reads, errors=errors, state_changes=len(changes), distinct_cloud_updates=len(distinct_updates))
        for c in changes:
            say(f"   change at {c}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        _pool.shutdown(wait=True)  # make sure every captured row is written before exit
