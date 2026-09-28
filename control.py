"""
Two manual, passkey-gated vehicle control actions, triggered only via the
dashboard's Status tab buttons -- never automatically.

STOP (still experimental): BYD's official stop-charging command is a
confirmed no-op. This uses a different mechanism instead -- setting a
smart-charging schedule whose window has just closed -- which was tested
live and DID actually pause charging. Deliberately does NOT clean up /
disable smart charging afterward anymore: an earlier version did, and
that cleanup step itself was what caused charging to silently resume
about a minute later (disabling smart charging reverts the car to its
default continuous-charging behavior). The paused state now persists
until you explicitly press Start.

START (confirmed working per pyBYD's own docs): calls the real
start_charging() command directly.

Required environment variables: same BYD_* credentials as poll.py, plus
NTFY_TOPIC, plus CONTROL_ACTION ("stop" or "start").
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from pybyd import BydClient, BydConfig
from scheduler import reset_time_after_stop

NTFY_TOPIC = os.environ.get("NTFY_TOPIC")
CONTROL_ACTION = os.environ.get("CONTROL_ACTION", "stop")
VEHICLE_TZ = ZoneInfo("Australia/Sydney")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")


def get_tracker_settings() -> dict:
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/tracker_settings?select=window_start_time,window_end_time,time_window_enabled",
        headers={"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"},
        timeout=15,
    )
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        raise RuntimeError("tracker_settings table is empty")
    return rows[0]


def notify(title: str, message: str) -> None:
    if not NTFY_TOPIC:
        print(f"notify skipped: NTFY_TOPIC not set. Would have sent: {title} - {message}")
        return
    try:
        resp = requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={"Title": title, "Priority": "high"},
            timeout=10,
        )
        print(f"ntfy notification sent: status={resp.status_code}")
    except Exception as exc:  # noqa: BLE001
        print(f"warning: ntfy notification failed: {exc}", file=sys.stderr)


def patch_settings(updates: dict) -> None:
    try:
        resp = requests.patch(
            f"{SUPABASE_URL}/rest/v1/tracker_settings?id=eq.true",
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
            },
            json=updates,
            timeout=15,
        )
        resp.raise_for_status()
    except Exception as exc:
        print(f"warning: failed to update tracker_settings {list(updates)}: {exc}", file=sys.stderr)


def review_soon() -> dict:
    """A manual start/stop should be followed by a quick check that it
    took effect, exactly like an automatic one."""
    return {"next_poll_at": (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
            "next_poll_reason": "reviewing manual command"}


def clear_manual_charge_override() -> None:
    """Manual Stop should always win -- if a 'Charge Now For X' override
    is still active, leaving it would mean the automation sees 'override
    active, not charging, plugged in' and silently restarts it. Also
    queues the car-schedule restore, since the stop trick overwrites the
    car's own schedule with a zero-width one, and asks for a quick
    review poll."""
    updates = {"manual_charge_until": None, **review_soon()}
    try:
        reset_at = reset_time_after_stop(datetime.now(timezone.utc), get_tracker_settings())
        if reset_at is not None:
            updates["schedule_reset_at"] = reset_at.isoformat()
    except Exception as exc:
        print(f"warning: could not queue schedule restore: {exc}", file=sys.stderr)
    patch_settings(updates)
    print("cleared manual_charge_until, queued schedule restore + review")


async def attempt_stop(client: BydClient, vin: str) -> None:
    clear_manual_charge_override()

    before = await client.get_charging_status(vin)
    print(f"charging state before stop attempt: {before.charging_state}, soc={before.soc}")

    now_local = datetime.now(VEHICLE_TZ)
    end_time_str = now_local.strftime("%H:%M")
    start_time_str = now_local.replace(second=0, microsecond=0).strftime("%H:%M")

    try:
        result = await client.save_charging_schedule(
            vin,
            start_charge_time=start_time_str,
            end_charge_time=end_time_str,
            charge_way="s",
            enabled=True,
        )
        print(f"save_charging_schedule result: {result}")
    except Exception as exc:
        print(f"save_charging_schedule raised: {exc}", file=sys.stderr)
        notify(
            "Stop attempt: command failed",
            f"save_charging_schedule errored: {exc}. Charging likely still active -- unplug manually if needed.",
        )
        return

    await asyncio.sleep(10)
    after = await client.get_charging_status(vin)
    print(f"charging state after stop attempt: {after.charging_state}, soc={after.soc}")

    still_charging = after.charging_state == before.charging_state and before.charging_state is not None
    if still_charging:
        notify(
            "Stop attempt: likely did NOT work",
            f"Schedule was set, but the car still reports the same charging state ({after.charging_state}). Unplug manually if you need it stopped.",
        )
    else:
        notify(
            "Charging stopped",
            f"State changed from {before.charging_state} to {after.charging_state}. It should stay stopped now -- press Start Charging on the dashboard when you want to resume.",
        )


async def attempt_start(client: BydClient, vin: str) -> None:
    patch_settings(review_soon())
    before = await client.get_charging_status(vin)
    print(f"charging state before start attempt: {before.charging_state}, soc={before.soc}")

    try:
        result = await client.start_charging(vin)
        print(f"start_charging result: {result}")
        notify(
            "Charging started",
            f"start_charging command succeeded. Check the dashboard to confirm.",
        )
    except Exception as exc:
        print(f"start_charging raised: {exc}", file=sys.stderr)
        notify(
            "Start attempt: command failed",
            f"start_charging errored: {exc}.",
        )


async def attempt_set_schedule(client: BydClient, vin: str) -> None:
    settings = get_tracker_settings()
    window_start = settings.get("window_start_time")
    window_end = settings.get("window_end_time")
    if not window_start or not window_end:
        notify("Set schedule: nothing to set", "window_start_time / window_end_time not configured in Settings.")
        return

    try:
        result = await client.save_charging_schedule(
            vin,
            start_charge_time=window_start,
            end_charge_time=window_end,
            charge_way="s",
            enabled=True,
        )
        print(f"save_charging_schedule result: {result}")
        patch_settings({"schedule_reset_at": None})
        notify(
            "Charging schedule set",
            f"Car's native scheduled charging set to {window_start}\u2013{window_end}, matching your configured window.",
        )
    except Exception as exc:
        print(f"save_charging_schedule raised: {exc}", file=sys.stderr)
        notify("Set schedule: command failed", f"save_charging_schedule errored: {exc}.")


async def main() -> None:
    config = BydConfig.from_env()
    async with BydClient(config) as client:
        vehicles = await client.get_vehicles()
        if not vehicles:
            notify("Control attempt failed", "No vehicle found on this account.")
            return
        vin = vehicles[0].vin

        if CONTROL_ACTION == "start":
            await attempt_start(client, vin)
        elif CONTROL_ACTION == "set-schedule":
            await attempt_set_schedule(client, vin)
        else:
            await attempt_stop(client, vin)


if __name__ == "__main__":
    asyncio.run(main())
