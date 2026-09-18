"""
Polls the BYD vehicle API (via pyBYD) once, writes a telemetry snapshot to
Supabase, and opens/closes a charging_sessions row whenever a charging
start/stop transition is detected between this poll and the previous one.

Run on a schedule (see .github/workflows/poll.yml) — every invocation is a
single poll-and-exit, not a long-running loop.

Required environment variables:
    BYD_USERNAME, BYD_PASSWORD        - BYD account credentials
    BYD_COUNTRY_CODE                  - e.g. "AU"
    BYD_CONTROL_PIN                   - optional, only needed for remote commands
    SUPABASE_URL                      - e.g. https://xxxx.supabase.co
    SUPABASE_SERVICE_KEY              - service_role key (bypasses RLS). Never
                                         the anon/publishable key — this script
                                         needs write access.

Field notes (confirmed against pyBYD 0.0.75 source, not guessed):
    - Battery %:      ChargingStatus.soc (preferred) or realtime.elec_percent (fallback)
    - Charging state: ChargingStatus.charging_state (preferred) or
                       realtime.charging_state enum (fallback)
    - Odometer:       realtime.total_mileage
    - GPS:             gps.latitude / gps.longitude

    charging_power_kw is populated from realtime.power_battery as a
    best-effort value. Its exact sign/unit convention is NOT confirmed
    against a live car yet (pyBYD's own docs note some fields are still
    being mapped) — treat it as informational only until verified. Cost
    calculations in this script rely solely on battery % delta, not on
    this field, so it does not affect accuracy of the cost/km numbers.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

import requests
from pybyd import BydClient, BydConfig
from pybyd.models.realtime import ChargingState

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}


def sb_get(table: str, params: dict) -> list[dict]:
    resp = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def sb_insert(table: str, row: dict) -> dict:
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**HEADERS, "Prefer": "return=representation"},
        json=row,
        timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    return result[0] if isinstance(result, list) else result


def sb_patch(table: str, row_id: int, updates: dict) -> None:
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
        params={"id": f"eq.{row_id}"},
        json=updates,
        timeout=15,
    )
    resp.raise_for_status()


def get_last_snapshot() -> dict | None:
    rows = sb_get("vehicle_snapshots", {"order": "recorded_at.desc", "limit": "1"})
    return rows[0] if rows else None


def get_open_session() -> dict | None:
    rows = sb_get("charging_sessions", {"ended_at": "is.null", "order": "started_at.desc", "limit": "1"})
    return rows[0] if rows else None


def get_last_closed_session() -> dict | None:
    rows = sb_get(
        "charging_sessions",
        {"ended_at": "not.is.null", "order": "ended_at.desc", "limit": "1"},
    )
    return rows[0] if rows else None


def get_tracker_settings() -> dict:
    rows = sb_get("tracker_settings", {"select": "*"})
    if not rows:
        raise RuntimeError("tracker_settings table is empty — expected exactly one row")
    return rows[0]


async def fetch_vehicle_state() -> dict:
    config = BydConfig.from_env()
    async with BydClient(config) as client:
        vehicles = await client.get_vehicles()
        if not vehicles:
            raise RuntimeError("No vehicles returned for this BYD account")
        vin = vehicles[0].vin

        realtime = await client.get_vehicle_realtime(vin)
        charging = await client.get_charging_status(vin)

        try:
            gps = await client.get_gps_info(vin)
            latitude, longitude = gps.latitude, gps.longitude
        except Exception as exc:  # noqa: BLE001 - GPS is best-effort
            print(f"warning: GPS fetch failed, continuing without it: {exc}", file=sys.stderr)
            latitude = longitude = None

        # Prefer the dedicated charging endpoint's values; fall back to the
        # general realtime blob if a field is missing there.
        battery_pct = charging.soc if charging.soc is not None else realtime.elec_percent

        if charging.charging_state is not None:
            is_charging = charging.charging_state == ChargingState.CHARGING.value
        else:
            is_charging = realtime.charging_state == ChargingState.CHARGING

        return {
            "battery_pct": battery_pct,
            "is_charging": is_charging,
            "charging_power_kw": realtime.power_battery,
            "odometer_km": realtime.total_mileage,
            "latitude": latitude,
            "longitude": longitude,
            "raw": {
                "realtime": realtime.model_dump(mode="json"),
                "charging": charging.model_dump(mode="json"),
            },
        }


def main() -> None:
    state = asyncio.run(fetch_vehicle_state())
    now = datetime.now(timezone.utc).isoformat()

    prev_snapshot = get_last_snapshot()
    prev_is_charging = bool(prev_snapshot["is_charging"]) if prev_snapshot else None

    # Always record the snapshot, regardless of transition.
    sb_insert("vehicle_snapshots", {"recorded_at": now, **state})
    print(f"snapshot written: battery={state['battery_pct']}% charging={state['is_charging']}")

    current_is_charging = bool(state["is_charging"])

    # Charging just started
    if not prev_is_charging and current_is_charging:
        existing_open = get_open_session()
        if existing_open is None:
            sb_insert(
                "charging_sessions",
                {
                    "started_at": now,
                    "start_pct": state["battery_pct"],
                    "start_odometer_km": state["odometer_km"],
                },
            )
            print("charging session opened")
        else:
            print("charging session already open, skipping open")

    # Charging just stopped
    elif prev_is_charging and not current_is_charging:
        open_session = get_open_session()
        if open_session is None:
            print("warning: charging stopped but no open session found, nothing to close", file=sys.stderr)
        else:
            settings = get_tracker_settings()
            start_pct = open_session["start_pct"]
            end_pct = state["battery_pct"]
            pct_delta = (end_pct - start_pct) if (start_pct is not None and end_pct is not None) else None
            energy_added_kwh = (
                (pct_delta / 100.0) * settings["battery_capacity_kwh"] if pct_delta is not None else None
            )

            last_closed = get_last_closed_session()
            km_since_last_charge = None
            if last_closed and last_closed.get("end_odometer_km") is not None and state["odometer_km"] is not None:
                km_since_last_charge = open_session["start_odometer_km"] - last_closed["end_odometer_km"]

            sb_patch(
                "charging_sessions",
                open_session["id"],
                {
                    "ended_at": now,
                    "end_pct": end_pct,
                    "end_odometer_km": state["odometer_km"],
                    "energy_added_kwh": energy_added_kwh,
                    "electricity_rate": settings["electricity_rate_per_kwh"],
                    "km_since_last_charge": km_since_last_charge,
                },
            )
            print(f"charging session closed: {pct_delta}% added, {energy_added_kwh} kWh")


if __name__ == "__main__":
    main()
