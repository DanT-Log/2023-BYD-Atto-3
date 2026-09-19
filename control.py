"""
EXPERIMENTAL, attempt #2: the toggle_smart_charging(enable=False) approach
was tried live and confirmed NOT to stop an active charge (charging_state
stayed unchanged before/after). This tries a different mechanism: setting
a smart-charging schedule (a genuinely different endpoint, saveOrUpdate,
not the confirmed-broken changeChargeStatue) whose time window ends right
now. The idea: if the car's onboard controller evaluates this schedule
locally rather than needing a live command continuously honored, closing
the window might actually stop charging where the direct toggle didn't.

Still genuinely untested. May not work either. If it doesn't, physically
unplugging remains the only confirmed-working option.

Cleanup: regardless of outcome, disables smart charging afterward so this
one-off schedule doesn't linger and unexpectedly affect the next charge.
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


async def attempt_stop() -> None:
    config = BydConfig.from_env()
    async with BydClient(config) as client:
        vehicles = await client.get_vehicles()
        if not vehicles:
            notify("Stop attempt failed", "No vehicle found on this account.")
            return
        vin = vehicles[0].vin

        before = await client.get_charging_status(vin)
        print(f"charging state before attempt: {before.charging_state}, soc={before.soc}")

        now_local = datetime.now(VEHICLE_TZ)
        end_time_str = now_local.strftime("%H:%M")
        # A minute-wide window that has essentially already closed.
        start_time_str = (now_local.replace(second=0, microsecond=0)).strftime("%H:%M")

        try:
            result = await client.save_charging_schedule(
                vin,
                start_charge_time=start_time_str,
                end_charge_time=end_time_str,
                charge_way="s",  # single/one-off, not a recurring daily schedule
                enabled=True,
            )
            print(f"save_charging_schedule result: {result}")
        except Exception as exc:
            print(f"save_charging_schedule raised: {exc}", file=sys.stderr)
            notify(
                "Stop attempt #2: command failed",
                f"save_charging_schedule errored: {exc}. Charging likely still active -- unplug manually if needed.",
            )
            return

        await asyncio.sleep(10)
        after = await client.get_charging_status(vin)
        print(f"charging state after attempt: {after.charging_state}, soc={after.soc}")

        still_charging = after.charging_state == before.charging_state and before.charging_state is not None

        # Cleanup regardless of outcome: disable smart charging so this
        # one-off schedule doesn't linger and affect the next real charge.
        try:
            await client.toggle_smart_charging(vin, enable=False)
            print("cleanup: smart charging disabled after attempt")
        except Exception as exc:
            print(f"cleanup toggle failed (non-fatal): {exc}", file=sys.stderr)

        if still_charging:
            notify(
                "Stop attempt #2: likely did NOT work",
                f"Schedule was set with an already-closed window, but the car still reports the same charging state ({after.charging_state}). This method may not work either -- unplug manually if you need it stopped.",
            )
        else:
            notify(
                "Stop attempt #2: charging state changed",
                f"Charging state changed from {before.charging_state} to {after.charging_state}. Check the dashboard to confirm it actually stopped.",
            )


if __name__ == "__main__":
    asyncio.run(attempt_stop())
