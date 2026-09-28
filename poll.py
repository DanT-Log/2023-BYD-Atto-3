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

    charging_power_kw comes from the "chargePower" field in the raw API
    response (unit confirmed via the accompanying "chargePowerUnit": "kW").
    This field isn't yet mapped as a typed attribute on pyBYD's
    ChargingStatus model as of 0.0.75, so it's read directly from
    `.raw`. energy_added_kwh is computed by trapezoidal integration of
    this value across every snapshot taken during a charging session
    (i.e. actual metered energy over time), falling back to a
    battery-capacity-% estimate only if fewer than two power readings
    are available for that session.
"""

from __future__ import annotations

import asyncio
import math
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from pybyd import BydClient, BydConfig
from pybyd.models.realtime import ChargingState
from scheduler import poll_step

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}


def sb_get(table: str, params: dict | list[tuple[str, str]]) -> list[dict]:
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


def parse_ts(s: str) -> datetime:
    s = s.strip()
    if re.search(r"[+-]\d{2}$", s):  # e.g. "...+00" -> "...+00:00"
        s += ":00"
    return datetime.fromisoformat(s)


def integrate_energy_kwh(session_start: str, session_end: str) -> float | None:
    """Trapezoidal integration of charging_power_kw across every snapshot
    taken between session_start and session_end. Returns None if fewer
    than two usable power readings exist, so the caller can fall back to
    the %-based estimate.

    The final snapshot in a session (the one that detected charging had
    stopped) always has charging_power_kw = null by definition — the car
    wasn't charging anymore when it was taken. Rather than dropping that
    trailing gap entirely (which silently loses however long it had been
    since the last real reading), the last known charging power is held
    flat through to session_end. This assumes power stayed roughly level
    right up to disconnect, which is the best available estimate without
    a real reading in that window.
    """
    snapshots = sb_get(
        "vehicle_snapshots",
        [
            ("recorded_at", f"gte.{session_start}"),
            ("recorded_at", f"lte.{session_end}"),
            ("order", "recorded_at.asc"),
        ],
    )
    usable = [s for s in snapshots if s.get("charging_power_kw") is not None]
    if len(usable) < 2:
        return None

    total_kwh = 0.0
    for prev, curr in zip(usable, usable[1:]):
        t0, t1 = parse_ts(prev["recorded_at"]), parse_ts(curr["recorded_at"])
        hours = (t1 - t0).total_seconds() / 3600.0
        if hours <= 0:
            continue
        p0, p1 = float(prev["charging_power_kw"]), float(curr["charging_power_kw"])
        total_kwh += (p0 + p1) / 2.0 * hours

    # Hold the last known power flat from the final real reading to the
    # actual session end, instead of dropping that trailing gap.
    last_reading_t = parse_ts(usable[-1]["recorded_at"])
    end_t = parse_ts(session_end)
    trailing_hours = (end_t - last_reading_t).total_seconds() / 3600.0
    if trailing_hours > 0:
        total_kwh += float(usable[-1]["charging_power_kw"]) * trailing_hours

    return total_kwh


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/lon points, in metres."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(d_lambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def classify_location(lat: float | None, lon: float | None, settings: dict) -> str:
    """Returns 'home', 'public', or 'unknown' based on distance from the
    home coordinates stored in tracker_settings.
    """
    home_lat, home_lon = settings.get("home_latitude"), settings.get("home_longitude")
    if lat is None or lon is None or home_lat is None or home_lon is None:
        return "unknown"
    distance = haversine_m(lat, lon, home_lat, home_lon)
    return "home" if distance <= settings["home_radius_m"] else "public"


NTFY_TOPIC = os.environ.get("NTFY_TOPIC")
VEHICLE_TZ = ZoneInfo("Australia/Sydney")


def notify_auto_stop(success: bool, pct: float, reason: str) -> None:
    if not NTFY_TOPIC:
        print("notify skipped: NTFY_TOPIC is not set", file=sys.stderr)
        return

    if success:
        title = "BYD Atto 3 \u2014 auto-stop sent"
        message = f"Battery at {pct}% \u2014 {reason}, stop command sent. Will retry next poll if it doesn't take effect."
    else:
        title = "BYD Atto 3 \u2014 auto-stop FAILED"
        message = f"Battery at {pct}% \u2014 {reason}, but the stop command errored. Will retry next poll \u2014 check manually if this keeps failing."

    try:
        resp = requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={"Title": title, "Priority": "high"},
            timeout=10,
        )
        print(f"ntfy auto-stop notification sent: status={resp.status_code}")
    except Exception as exc:  # noqa: BLE001
        print(f"warning: ntfy notification failed: {exc}", file=sys.stderr)


def notify_auto_start(success: bool, pct: float, reason: str) -> None:
    if not NTFY_TOPIC:
        print("notify skipped: NTFY_TOPIC is not set", file=sys.stderr)
        return

    if success:
        title = "BYD Atto 3 \u2014 auto-start sent"
        message = f"Battery at {pct}% \u2014 {reason}, start command sent."
    else:
        title = "BYD Atto 3 \u2014 auto-start FAILED"
        message = f"Battery at {pct}% \u2014 {reason}, but the start command errored. Will retry next poll."

    try:
        resp = requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={"Title": title, "Priority": "default"},
            timeout=10,
        )
        print(f"ntfy auto-start notification sent: status={resp.status_code}")
    except Exception as exc:  # noqa: BLE001
        print(f"warning: ntfy notification failed: {exc}", file=sys.stderr)


def notify_schedule_sync_failed(window_start: str, window_end: str, exc: Exception) -> None:
    """Deliberately lower priority than the stop/start notifications --
    this means the car's own native scheduled-charging display might be
    briefly stale, not that the actual stop or start command failed.
    Will simply be retried on the next auto-stop or auto-start."""
    if not NTFY_TOPIC:
        print("notify skipped: NTFY_TOPIC is not set", file=sys.stderr)
        return
    title = "BYD Atto 3 \u2014 schedule sync failed (action itself still worked)"
    message = f"Tried to sync the car's native schedule to {window_start}\u2013{window_end} but it errored: {exc}. Will retry on the next stop/start."
    try:
        resp = requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={"Title": title, "Priority": "low"},
            timeout=10,
        )
        print(f"ntfy schedule-sync-failed notification sent: status={resp.status_code}")
    except Exception as notify_exc:  # noqa: BLE001
        print(f"warning: ntfy notification failed: {notify_exc}", file=sys.stderr)


async def attempt_auto_stop_async() -> None:
    """Stop charging via the schedule-window-closed trick (a zero-width
    now-now schedule) -- confirmed live to actually pause charging. It
    OVERWRITES the car's own native schedule; scheduler.py queues a
    restore (schedule_reset_at) for a safe moment afterwards."""
    config = BydConfig.from_env()
    async with BydClient(config) as client:
        vehicles = await client.get_vehicles()
        vin = vehicles[0].vin
        now_local = datetime.now(VEHICLE_TZ)
        await client.save_charging_schedule(
            vin,
            start_charge_time=now_local.replace(second=0, microsecond=0).strftime("%H:%M"),
            end_charge_time=now_local.strftime("%H:%M"),
            charge_way="s",
            enabled=True,
        )


async def attempt_auto_start_async() -> None:
    """Officially documented start_charging() -- confirmed working, and
    unaffected by whatever schedule the car currently holds."""
    config = BydConfig.from_env()
    async with BydClient(config) as client:
        vehicles = await client.get_vehicles()
        await client.start_charging(vehicles[0].vin)


async def restore_schedule_async(window_start: str, window_end: str) -> None:
    """Put the car's own scheduled-charging window back to the configured
    one. Separate from stop/start on purpose: BYD's cloud can time out on
    this specific call, and that must never look like the stop failed."""
    config = BydConfig.from_env()
    async with BydClient(config) as client:
        vehicles = await client.get_vehicles()
        await client.save_charging_schedule(
            vehicles[0].vin,
            start_charge_time=window_start,
            end_charge_time=window_end,
            charge_way="s",
            enabled=True,
        )


def notify_gave_up(action: str, pct: float | None, attempts: int) -> None:
    if not NTFY_TOPIC:
        return
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=f"Sent {attempts} {action} commands and the car still hasn't responded (battery {pct}%). Pausing retries for a while \u2014 check the car/charger.".encode("utf-8"),
            headers={"Title": f"BYD Atto 3 \u2014 {action} not taking effect", "Priority": "high"},
            timeout=10,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"warning: ntfy notification failed: {exc}", file=sys.stderr)


def notify_charge_started(location_type: str, start_pct: float | None) -> None:
    if not NTFY_TOPIC:
        print("notify skipped: NTFY_TOPIC is not set", file=sys.stderr)
        return

    place = "Home" if location_type == "home" else ("Public" if location_type == "public" else "Unknown location")
    pct_str = f"{start_pct}%" if start_pct is not None else "unknown battery %"
    message = f"{place} charging started at {pct_str}."

    try:
        resp = requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={
                "Title": f"BYD Atto 3 \u2014 {place.lower()} charging started",
                "Priority": "default",
            },
            timeout=10,
        )
        print(f"ntfy start notification sent: status={resp.status_code}")
    except Exception as exc:  # noqa: BLE001 - notification failure shouldn't break the poll
        print(f"warning: ntfy notification failed: {exc}", file=sys.stderr)


def notify_charge_finished(
    location_type: str,
    start_pct: float | None,
    end_pct: float | None,
    range_added_km: float | None,
    energy_kwh: float | None,
    cost: float | None,
    rate_confirmed: bool,
    session_id: int,
) -> None:
    if not NTFY_TOPIC:
        print("notify skipped: NTFY_TOPIC is not set", file=sys.stderr)
        return

    pct_str = f"{start_pct}% \u2192 {end_pct}%" if (start_pct is not None and end_pct is not None) else "unknown %"
    range_str = f"{range_added_km:+.0f} km range" if range_added_km is not None else "range unknown"
    energy_str = f"{energy_kwh:.1f} kWh" if energy_kwh is not None else "unknown kWh"
    cost_str = f"${cost:.2f}" if cost is not None else "unknown cost"
    place = "Home" if location_type == "home" else ("Public" if location_type == "public" else "Unknown location")

    message = f"{place} charge finished: {pct_str}, {range_str}, {energy_str} added, {cost_str}."
    if not rate_confirmed:
        message += f" Rate is a default estimate \u2014 reply to Claude to set the real rate for session #{session_id}."

    try:
        resp = requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={
                "Title": f"BYD Atto 3 \u2014 {place.lower()} charge done",
                "Priority": "default",
            },
            timeout=10,
        )
        print(f"ntfy notification sent: status={resp.status_code}")
    except Exception as exc:  # noqa: BLE001 - notification failure shouldn't break the poll
        print(f"warning: ntfy notification failed: {exc}", file=sys.stderr)


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

        # "chargePower" (kW) lives in the raw response but isn't yet a typed
        # field on ChargingStatus in pyBYD 0.0.75 — pull it out directly.
        charging_raw = charging.raw if isinstance(charging.raw, dict) else {}
        charging_power_kw = None
        raw_power = charging_raw.get("chargePower")
        if raw_power is not None:
            try:
                charging_power_kw = float(raw_power)
            except (TypeError, ValueError):
                charging_power_kw = None

        # connectState: whether the charging cable is physically plugged
        # in, distinct from is_charging (plugged in but paused/scheduled
        # counts as connected but not charging). Same raw dict and same
        # camelCase convention already confirmed working for chargePower
        # above -- not independently verified the way the door locks
        # were, but the failure mode here is low-stakes: worst case is a
        # harmless no-op start command if nothing's actually plugged in.
        connect_state = charging_raw.get("connectState")

        return {
            "battery_pct": battery_pct,
            "is_charging": is_charging,
            "charging_power_kw": charging_power_kw,
            "connect_state": connect_state,
            "odometer_km": realtime.total_mileage,
            "range_km": realtime.ev_endurance,
            # Time-to-full comes from the dedicated charging endpoint (ChargingStatus),
            # not the general realtime blob -- the latter's equivalent field is
            # frequently unset (pyBYD nulls out BYD's -1 "not applicable" sentinel).
            "full_hour": charging.full_hour,
            "full_minute": charging.full_minute,
            # A genuine countdown duration (hours/minutes remaining until full),
            # distinct from full_hour/full_minute above which is an ambiguous
            # clock-time-of-day with no date attached. Combined with this
            # snapshot's own timestamp, this gives an unambiguous target
            # date+time with no guessing about which day it lands on.
            "remaining_hours": realtime.remaining_hours,
            "remaining_minutes": realtime.remaining_minutes,
            # Car's own reported lifetime average efficiency, for comparison against
            # our independently tracked figure (energy added vs km driven).
            "car_reported_kwh_per_100km": realtime.total_consumption_ev,
            "latitude": latitude,
            "longitude": longitude,
            "raw": {
                "realtime": realtime.model_dump(mode="json"),
                "charging": charging.model_dump(mode="json"),
            },
        }


def get_poll_interval_minutes() -> int:
    """Reads the user-configurable poll interval from tracker_settings.
    Defaults to 5 (matching the underlying pg_cron trigger rate) if the
    setting is missing or invalid for any reason -- never let a bad
    reading here silently disable polling.
    """
    try:
        rows = sb_get("tracker_settings", {"select": "poll_interval_minutes"})
        value = rows[0].get("poll_interval_minutes") if rows else None
        return int(value) if value else 5
    except Exception as exc:  # noqa: BLE001
        print(f"warning: could not read poll_interval_minutes, defaulting to 5: {exc}", file=sys.stderr)
        return 5


def recent_standby_rate() -> float | None:
    """%/hour lost while parked (odometer unchanged, not charging) over
    the last 48h -- same isolation as the dashboard's Standby Loss."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    rows = sb_get("vehicle_snapshots", {
        "recorded_at": f"gte.{cutoff}",
        "select": "recorded_at,battery_pct,odometer_km,is_charging",
        "order": "recorded_at.asc",
    })
    drop_total = hours_total = 0.0
    for a, b in zip(rows, rows[1:]):
        if a["is_charging"] or b["is_charging"]:
            continue
        if None in (a["odometer_km"], b["odometer_km"], a["battery_pct"], b["battery_pct"]):
            continue
        if float(b["odometer_km"]) != float(a["odometer_km"]):
            continue
        drop = float(a["battery_pct"]) - float(b["battery_pct"])
        if drop <= 0:
            continue
        drop_total += drop
        hours_total += (parse_ts(b["recorded_at"]) - parse_ts(a["recorded_at"])).total_seconds() / 3600
    return drop_total / hours_total if hours_total > 0 else None


def main() -> None:
    # No throttle here any more: a poll only runs when the scheduler says
    # it is due (pg_cron poll_tick) or someone forced it. What to do next,
    # and when to poll again, is decided in scheduler.py.
    prev_snapshot = get_last_snapshot()
    settings = get_tracker_settings()

    state = asyncio.run(fetch_vehicle_state())
    now = datetime.now(timezone.utc).isoformat()

    prev_is_charging = bool(prev_snapshot["is_charging"]) if prev_snapshot else None

    # Always record the snapshot, regardless of transition.
    # connect_state is used for the live auto-start decision below but
    # was never added as a column on vehicle_snapshots -- inserting it
    # at the top level 400s against Postgrest's schema. It's already
    # captured separately inside state["raw"]["charging"] for the
    # dashboard's plugged-in indicator, so this exclusion doesn't lose
    # any data, just avoids inserting a column that doesn't exist.
    snapshot_fields = {k: v for k, v in state.items() if k != "connect_state"}
    sb_insert("vehicle_snapshots", {"recorded_at": now, **snapshot_fields})
    print(f"snapshot written: battery={state['battery_pct']}% charging={state['is_charging']}")

    current_is_charging = bool(state["is_charging"])

    # Charging just started
    if not prev_is_charging and current_is_charging:
        existing_open = get_open_session()
        if existing_open is None:
            location_type = classify_location(state["latitude"], state["longitude"], settings)
            sb_insert(
                "charging_sessions",
                {
                    "started_at": now,
                    "start_pct": state["battery_pct"],
                    "start_odometer_km": state["odometer_km"],
                    "start_range_km": state["range_km"],
                    "start_latitude": state["latitude"],
                    "start_longitude": state["longitude"],
                    "location_type": location_type,
                },
            )
            print(f"charging session opened ({location_type})")
            notify_charge_started(location_type, state["battery_pct"])
        else:
            print("charging session already open, skipping open")

    # Charging just stopped
    elif prev_is_charging and not current_is_charging:
        open_session = get_open_session()
        if open_session is None:
            print("warning: charging stopped but no open session found, nothing to close", file=sys.stderr)
        else:
            start_pct = open_session["start_pct"]
            end_pct = state["battery_pct"]
            pct_delta = (end_pct - start_pct) if (start_pct is not None and end_pct is not None) else None

            energy_added_kwh = integrate_energy_kwh(open_session["started_at"], now)
            estimate_method = "integrated"
            if energy_added_kwh is None:
                energy_added_kwh = (
                    (pct_delta / 100.0) * settings["battery_capacity_kwh"] if pct_delta is not None else None
                )
                estimate_method = "pct_estimate"

            last_closed = get_last_closed_session()
            km_since_last_charge = None
            if last_closed and last_closed.get("end_odometer_km") is not None and state["odometer_km"] is not None:
                km_since_last_charge = open_session["start_odometer_km"] - last_closed["end_odometer_km"]

            start_range_km = open_session.get("start_range_km")
            end_range_km = state["range_km"]
            range_added_km = (
                end_range_km - start_range_km if (start_range_km is not None and end_range_km is not None) else None
            )

            location_type = open_session.get("location_type") or "unknown"
            if location_type == "home" and settings.get("home_rate_per_kwh") is not None:
                electricity_rate = settings["home_rate_per_kwh"]
                rate_confirmed = True
            elif location_type == "public" and settings.get("public_rate_per_kwh") is not None:
                electricity_rate = settings["public_rate_per_kwh"]
                rate_confirmed = False  # default estimate, not a confirmed real rate
            else:
                electricity_rate = settings.get("home_rate_per_kwh")  # last-resort fallback
                rate_confirmed = False

            sb_patch(
                "charging_sessions",
                open_session["id"],
                {
                    "ended_at": now,
                    "end_pct": end_pct,
                    "end_odometer_km": state["odometer_km"],
                    "end_range_km": end_range_km,
                    "range_added_km": range_added_km,
                    "energy_added_kwh": energy_added_kwh,
                    "electricity_rate": electricity_rate,
                    "km_since_last_charge": km_since_last_charge,
                    "rate_confirmed": rate_confirmed,
                },
            )
            print(
                f"charging session closed: {pct_delta}% added, "
                f"{energy_added_kwh} kWh @ {location_type} rate ({estimate_method})"
            )

            cost = energy_added_kwh * electricity_rate if (energy_added_kwh is not None and electricity_rate is not None) else None
            notify_charge_finished(
                location_type=location_type,
                start_pct=start_pct,
                end_pct=end_pct,
                range_added_km=range_added_km,
                energy_kwh=energy_added_kwh,
                cost=cost,
                rate_confirmed=rate_confirmed,
                session_id=open_session["id"],
            )

    # Auto-stop: if a limit is configured and the car is currently
    # charging at or above it, attempt to stop. Checked every poll while
    # the condition holds (not just once) -- a command that doesn't take
    # effect immediately (BYD's cloud has a 1-2 min propagation delay,
    # confirmed during earlier testing) gets retried on the next poll
    # instead of silently giving up after one attempt.
    # ---- charge automation + when to poll next (all logic in scheduler.py) ----
    def _ts(v):
        return parse_ts(v) if v else None

    now_dt = parse_ts(now)
    step_settings = dict(settings)
    step_settings["manual_charge_until"] = _ts(settings.get("manual_charge_until"))
    ctl = {
        "last_command": settings.get("last_command"),
        "last_command_at": _ts(settings.get("last_command_at")),
        "command_attempts": settings.get("command_attempts") or 0,
        "schedule_reset_at": _ts(settings.get("schedule_reset_at")),
    }
    step_state = {
        "is_charging": current_is_charging,
        "battery_pct": state["battery_pct"],
        "charging_power_kw": state.get("charging_power_kw"),
        "plugged": state.get("connect_state") not in (None, 0, "0"),
    }

    def send_command(action: str) -> bool:
        try:
            asyncio.run(attempt_auto_start_async() if action == "start" else attempt_auto_stop_async())
            print(f"auto-{action}: command sent")
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"auto-{action}: command failed: {exc}", file=sys.stderr)
            return False

    def restore_schedule(window_start: str, window_end: str) -> bool:
        try:
            asyncio.run(restore_schedule_async(window_start, window_end))
            print(f"car schedule restored to {window_start}-{window_end}")
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"schedule restore failed: {exc}", file=sys.stderr)
            return False

    def notify(kind: str, **info) -> None:
        if kind == "command":
            fn = notify_auto_start if info["action"] == "start" else notify_auto_stop
            fn(info["ok"], info["pct"], info["reason"])
        elif kind == "gave_up":
            notify_gave_up(info["action"], info["pct"], info["attempts"])
        elif kind == "schedule_sync_failed":
            notify_schedule_sync_failed(info["start"], info["end"], RuntimeError("BYD did not confirm the change"))

    result = poll_step(
        now=now_dt,
        state=step_state,
        settings=step_settings,
        ctl=ctl,
        prev_recorded_at=_ts(prev_snapshot["recorded_at"]) if prev_snapshot else None,
        standby_rate_fn=recent_standby_rate,
        send_command=send_command,
        restore_schedule=restore_schedule,
        notify=notify,
    )

    new_ctl = result["ctl"]
    updates = {
        "next_poll_at": result["next_poll_at"].isoformat(),
        "next_poll_reason": result["reason"],
        "last_command": new_ctl.get("last_command"),
        "last_command_at": new_ctl["last_command_at"].isoformat() if new_ctl.get("last_command_at") else None,
        "command_attempts": new_ctl.get("command_attempts") or 0,
        "schedule_reset_at": new_ctl["schedule_reset_at"].isoformat() if new_ctl.get("schedule_reset_at") else None,
    }
    if result["clear_override"]:
        updates["manual_charge_until"] = None
    sb_patch("tracker_settings", "true", updates)
    print(f"next poll: {updates['next_poll_at']} ({result['reason']})")


if __name__ == "__main__":
    main()
