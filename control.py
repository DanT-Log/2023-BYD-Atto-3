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
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from pybyd import BydClient, BydConfig

NTFY_TOPIC = os.environ.get("NTFY_TOPIC")
CONTROL_ACTION = os.environ.get("CONTROL_ACTION", "stop")
VEHICLE_TZ = ZoneInfo("Australia/Sydney")


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


async def attempt_stop(client: BydClient, vin: str) -> None:
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
        else:
            await attempt_stop(client, vin)


if __name__ == "__main__":
    asyncio.run(main())
