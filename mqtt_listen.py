"""Listen for BYD MQTT pushes for a fixed window and log what actually arrives.

One-off diagnostic, NOT part of the normal poller. The question it answers:
does BYD push car-originated events (charging started/stopped, plug in/out,
lock/unlock) to the MQTT channel pyBYD already connects to, for THIS car,
without us polling?

How the window is laid out, so the result is unambiguous:

  T+0:00  Log in. Read (and, if it is off, try to enable) the server-side
          vehicle-status push switch (type 701). Start MQTT and subscribe.
  T+1:00  ONE deliberate realtime request, clearly marked. Its reply comes
          back over MQTT, which proves the channel decodes end to end.
          Without this control, "nothing arrived" could mean either "BYD
          does not push" or "our decoding is broken", and we could not
          tell which.
  T+2:00  Quiet window. From here this script sends NOTHING, so anything
          that arrives was pushed by BYD on its own initiative.

Privacy: the repo and its Actions logs are public. Raw payloads can contain
GPS and account identifiers, so they go ONLY to the private
mqtt_test_events table (RLS on, no policies, service key only). The public
log shows event names, key names, and a short allowlist of non-identifying
scalar values.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from importlib import metadata
from typing import Any
from zoneinfo import ZoneInfo

import requests
from pybyd import BydClient, BydConfig

LISTEN_MINUTES = float(os.environ.get("LISTEN_MINUTES") or 20)
RUN_LABEL = os.environ.get("RUN_LABEL") or "test"
CONTROL_POLL_AT_S = float(os.environ.get("CONTROL_POLL_AT_S") or 60)
QUIET_FROM_S = float(os.environ.get("QUIET_FROM_S") or 120)
HEARTBEAT_S = float(os.environ.get("HEARTBEAT_S") or 60)
TICK_S = float(os.environ.get("TICK_S") or 5)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
TZ = ZoneInfo("Australia/Sydney")
START = time.monotonic()

_pool = ThreadPoolExecutor(max_workers=2)  # thread-safe: callbacks arrive from paho's thread too
_counts: dict[str, int] = {}
_quiet_counts: dict[str, int] = {}
_quiet = False

# Values are only printed to the public log for keys that look like state
# (charging, connect, lock, power...) and never for anything that could
# identify the car, the account, or where it is.
_SAFE_KEY = re.compile(r"charg|connect|percent|soc|power|lock|door|window|trunk|speed|gear|ready|online|state|status|code|msg", re.I)
_DENY_KEY = re.compile(r"lat|lng|lon|gps|vin|addr|location|user|token|key|serial|uuid|phone|mail|plate|name", re.I)


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
            print(f"warning: could not store event (HTTP {resp.status_code})", file=sys.stderr, flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"warning: could not store event ({type(exc).__name__})", file=sys.stderr, flush=True)


def store(event: str, payload: Any, topic: str | None = None) -> None:
    _pool.submit(_post_row, {"run_label": RUN_LABEL, "event": event, "topic": topic, "payload": payload})


def meta(kind: str, **info: Any) -> None:
    """Script-originated timeline marker, stored alongside the real events
    so the whole run can be reconstructed from one table."""
    store("_meta", {"kind": kind, **info})
    say(f"-- {kind} {' '.join(f'{k}={v}' for k, v in info.items())}".rstrip())


def summarize(payload: Any) -> str:
    """Public-log-safe one-liner: key names plus allowlisted scalar values."""
    body: Any = payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            body = data.get("respondData") if isinstance(data.get("respondData"), dict) else data
    if not isinstance(body, dict):
        return f"(non-dict payload: {type(body).__name__})"
    shown = []
    for key in sorted(body):
        value = body[key]
        printable = isinstance(value, (int, float, bool)) or (isinstance(value, str) and len(value) <= 20)
        if printable and _SAFE_KEY.search(key) and not _DENY_KEY.search(key):
            shown.append(f"{key}={value}")
    return f"{len(body)} keys; " + (" ".join(shown) if shown else "no safe scalars")


class _PybydWatch(logging.Handler):
    """Surface only the few pyBYD log lines that explain MQTT state.

    pyBYD logs the interesting connection facts at DEBUG, and swallows MQTT
    startup failures entirely. Full DEBUG would print broker and topic
    details (which embed the account id) into a public log, so instead this
    handler lets through just the lines that matter, with arguments dropped
    where they would be identifying.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
            if record.levelno >= logging.WARNING:
                say(f"pybyd {record.levelname}: {msg[:200]}")
            elif msg.startswith("MQTT startup failed"):
                exc = record.exc_info[1] if record.exc_info else None
                info = f"{type(exc).__name__}: {str(exc)[:200]}" if exc else "unknown"
                meta("mqtt_startup_failed", error=info)
            elif msg.startswith("MQTT connected"):
                say(msg[:80])
            elif msg.startswith("MQTT subscribe"):
                say("MQTT subscribe sent")
            elif msg.startswith("MQTT disconnected"):
                say(msg[:80])
        except Exception:  # noqa: BLE001 - logging must never break the run
            pass


async def read_push_state(client: BydClient, vin: str) -> dict[str, Any]:
    try:
        state = await client.get_push_state(vin)
        return {
            "status_push_enabled": state.status_push_enabled,
            "switches": [{"type": s.type, "state": s.state} for s in state.switches],
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}


def install_hooks(client: BydClient) -> None:
    """Wrap the client's MQTT handlers BEFORE MQTT starts. The runtime is
    built from these bound attributes at startup, so instance-level wrappers
    are picked up, and they see every decrypted event (pyBYD's own
    on_mqtt_event callback silently drops payloads of an unexpected shape)."""
    orig_event = client._on_mqtt_event
    orig_connected = client._on_mqtt_connected
    orig_refused = client._on_mqtt_connack_refused
    orig_decrypt = client._schedule_mqtt_reauth

    def on_event(event: Any) -> None:
        try:
            name = str(event.event)
            _counts[name] = _counts.get(name, 0) + 1
            if _quiet:
                _quiet_counts[name] = _quiet_counts.get(name, 0) + 1
            say(f"EVENT {name} | {summarize(event.payload)}")
            store(name, event.payload, event.topic)
        except Exception as exc:  # noqa: BLE001
            say(f"event logging failed: {type(exc).__name__}")
        orig_event(event)

    def on_connected() -> None:
        meta("mqtt_connected")
        orig_connected()

    def on_refused(consecutive: int) -> None:
        meta("mqtt_connack_refused", consecutive=consecutive)
        orig_refused(consecutive)

    def on_decrypt_failed() -> None:
        meta("mqtt_decrypt_failed")
        orig_decrypt()

    client._on_mqtt_event = on_event
    client._on_mqtt_connected = on_connected
    client._on_mqtt_connack_refused = on_refused
    client._schedule_mqtt_reauth = on_decrypt_failed


def mqtt_running(client: BydClient) -> bool:
    runtime = getattr(client, "_mqtt_runtime", None)
    return bool(runtime is not None and runtime.is_running)


async def main() -> None:
    global _quiet
    watch = logging.getLogger("pybyd")
    watch.setLevel(logging.DEBUG)
    watch.addHandler(_PybydWatch())
    watch.propagate = False

    config = BydConfig.from_env()
    async with BydClient(config) as client:
        install_hooks(client)
        try:
            version = metadata.version("pybyd")
        except Exception:  # noqa: BLE001
            version = "unknown"
        meta("script_started", pybyd=version, minutes=LISTEN_MINUTES, label=RUN_LABEL)

        vehicles = await client.get_vehicles()
        vin = vehicles[0].vin  # never printed: public log

        before = await read_push_state(client, vin)
        meta("push_state_before", **before)
        if not before.get("status_push_enabled") and "error" not in before:
            try:
                ack = await client.set_push_state(vin, enable=True)
                meta("push_enable_result", ok=True, ack=str(ack)[:120])
            except Exception as exc:  # noqa: BLE001 - "not supported" is a valid, informative answer
                meta("push_enable_result", ok=False, error=f"{type(exc).__name__}: {str(exc)[:200]}")
            meta("push_state_after", **(await read_push_state(client, vin)))

        await client._ensure_mqtt_started()  # lazy by default; swallows its own failures
        meta("mqtt_started", running=mqtt_running(client))

        control_done = False
        last_heartbeat = last_restart = 0.0
        deadline = START + LISTEN_MINUTES * 60
        while time.monotonic() < deadline:
            await asyncio.sleep(TICK_S)
            elapsed = time.monotonic() - START

            if not control_done and elapsed >= CONTROL_POLL_AT_S:
                control_done = True
                meta("control_poll_start", note="one deliberate request; its MQTT reply proves decoding works")
                try:
                    await client.get_vehicle_realtime(vin)
                    meta("control_poll_done", ok=True)
                except Exception as exc:  # noqa: BLE001
                    meta("control_poll_done", ok=False, error=f"{type(exc).__name__}: {str(exc)[:200]}")

            if not _quiet and elapsed >= QUIET_FROM_S:
                _quiet = True
                meta("quiet_window_begins", note="script now sends nothing; anything arriving was pushed by BYD")

            if elapsed - last_heartbeat >= HEARTBEAT_S:
                last_heartbeat = elapsed
                say(f"heartbeat: mqtt_running={mqtt_running(client)} events_total={sum(_counts.values())} quiet_events={sum(_quiet_counts.values())}")

            if not mqtt_running(client) and elapsed - last_restart >= 60:
                last_restart = elapsed
                meta("mqtt_restart_attempt")
                await client._ensure_mqtt_started()

        meta("finished", events_total=sum(_counts.values()), by_event=_counts, quiet_window_by_event=_quiet_counts)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        _pool.shutdown(wait=True)  # make sure every captured event is written before exit
