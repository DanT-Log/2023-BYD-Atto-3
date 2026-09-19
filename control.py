"""
EXPERIMENTAL: attempts to stop an active charge by disabling smart
charging (toggle_smart_charging(enable=False)) rather than the broken
cloud stop-charge command (verified no-op -- see pyBYD's own docstring
on BydClient.stop_charging).

This is genuinely untested against a real Atto 3. It may or may not
actually pause an active charge -- toggling smart charging off is a
different subsystem than the broken direct stop command, but there's
no confirmation either way until this has actually been tried. Treat
every result as informative, not a promise.

Triggered manually via the dashboard's experimental "Try Stop Charging"
button (passkey-gated), never automatically.

Required environment variables: same BYD_* credentials as poll.py, plus
NTFY_TOPIC (to report the raw result, since this is a fire-and-forget
GitHub Actions run with no other feedback channel).
"""

from __future__ import annotations

import asyncio
import os
import sys

import requests
from pybyd import BydClient, BydConfig

NTFY_TOPIC = os.environ.get("NTFY_TOPIC")


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

        # Check current state first, purely for the notification's context.
        before = await client.get_charging_status(vin)
        print(f"charging state before attempt: {before.charging_state}, soc={before.soc}")

        try:
            result = await client.toggle_smart_charging(vin, enable=False)
            print(f"toggle_smart_charging(enable=False) result: {result}")
        except Exception as exc:
            print(f"toggle_smart_charging raised: {exc}", file=sys.stderr)
            notify(
                "Stop attempt: command failed",
                f"toggle_smart_charging errored: {exc}. Charging likely still active -- unplug manually if needed.",
            )
            return

        # Give the car a few seconds, then check whether it actually stopped.
        await asyncio.sleep(10)
        after = await client.get_charging_status(vin)
        print(f"charging state after attempt: {after.charging_state}, soc={after.soc}")

        still_charging = after.charging_state == before.charging_state and before.charging_state is not None
        if still_charging:
            notify(
                "Stop attempt: likely did NOT work",
                f"Command was sent and accepted, but the car still reports the same charging state ({after.charging_state}). This experiment may not work -- unplug manually if you need it stopped.",
            )
        else:
            notify(
                "Stop attempt: charging state changed",
                f"Charging state changed from {before.charging_state} to {after.charging_state} after the command. Check the dashboard to confirm it actually stopped.",
            )


if __name__ == "__main__":
    asyncio.run(attempt_stop())
