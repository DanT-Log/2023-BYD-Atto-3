"""Event-scheduled polling and charge-automation decisions.

Pure logic only: no network, no environment, and no clock reads (the
caller passes `now`). That is deliberate -- the exact code that runs in
production is the code that gets simulated end to end, so behaviour can
be checked against a whole day before it touches the car.

The model is deliberately small:

  1. Every poll works out what state the car SHOULD be in (charging or
     not) from the settings, and compares it with what it IS in.
  2. If they differ, review again in a couple of minutes, re-sending the
     command if the first one hasn't landed.
  3. If they match, sleep until the next event we can actually predict:
     a window edge, the predicted moment the battery hits the target or
     the restart threshold, a manual override ending, restoring the
     car's own schedule, or -- right after the phone reports the car just
     parked -- a short burst of frequent checks for the plug going in.

There is one place that decides when to poll next (plan_next). It
replaces the old pair of crons plus a stack of "approaching X" margins.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

VEHICLE_TZ = ZoneInfo("Australia/Sydney")

# --- tuning -------------------------------------------------------------
REVIEW_DELAY_MIN = 2            # re-check this soon after sending a start/stop
COMMAND_HOLD_MIN = 4            # don't re-send the SAME command inside this (BYD's
                                # cloud can take several minutes to apply one)
COMMAND_MAX_ATTEMPTS = 5        # then give up (and say so) rather than spin
COMMAND_EPISODE_MIN = 30        # attempt counter resets after this long
RESET_DELAY_MIN = 5             # restore the car's schedule this long after a stop / window end
RESET_RETRY_MIN = 10            # ...and retry this often if BYD's cloud times out
CHARGING_INTERVAL_MIN = 5       # max gap while charging at normal (granny) speed
FAST_CHARGING_INTERVAL_MIN = 1  # max gap while fast charging
FAST_CHARGE_KW_THRESHOLD = 2.0  # home AC is a steady 1.4-1.5kW; above 2 is public/fast
MIN_GAP = timedelta(seconds=60) # the cron ticks once a minute
BURST_GAP_MIN = 3               # after the phone reports the car just parked, check this often
                                # until a plug is seen (the window itself is burst_until)
FAST_GAP_MIN = 1                # after "Start charging" is pressed, check this often until the
                                # car confirms it is charging (the window is fast_until)
DEFAULT_STANDBY_RATE = 0.2      # %/h fallback if we can't measure recent drain


def time_in_window(now_local: datetime, start_str: str, end_str: str) -> bool:
    """Window membership, including windows that wrap midnight."""
    try:
        sh, sm = map(int, start_str.split(":"))
        eh, em = map(int, end_str.split(":"))
    except (ValueError, AttributeError):
        return False
    start_min, end_min = sh * 60 + sm, eh * 60 + em
    now_min = now_local.hour * 60 + now_local.minute
    if start_min == end_min:
        return True
    if start_min < end_min:
        return start_min <= now_min < end_min
    return now_min >= start_min or now_min < end_min


def next_occurrence(now_local: datetime, hhmm: str) -> datetime:
    """Next wall-clock instance of HH:MM strictly after now."""
    h, m = map(int, hhmm.split(":"))
    cand = now_local.replace(hour=h, minute=m, second=0, microsecond=0)
    if cand <= now_local:
        cand += timedelta(days=1)
    return cand


def reset_time_after_stop(now: datetime, settings: dict) -> datetime | None:
    """When to put the car's own schedule back after our stop trick
    clobbered it. Inside the window we must wait until it closes
    (restoring mid-window could let the car's schedule restart charging
    above the limit); outside it, a few minutes after the stop."""
    if not settings.get("time_window_enabled"):
        return None
    ws = settings.get("window_start_time") or "00:00"
    we = settings.get("window_end_time") or "06:00"
    now_local = now.astimezone(VEHICLE_TZ)
    if time_in_window(now_local, ws, we):
        return next_occurrence(now_local, we) + timedelta(minutes=RESET_DELAY_MIN)
    return now + timedelta(minutes=RESET_DELAY_MIN)


def decide_action(*, is_charging, pct, plugged, settings, in_window, prev_in_window,
                  override_active) -> tuple[str | None, str]:
    """('start' | 'stop' | None, reason). None means: no change wanted."""
    if pct is None:
        return None, ""
    pct_enabled = bool(settings.get("pct_limit_enabled"))
    time_enabled = bool(settings.get("time_window_enabled"))
    threshold = settings.get("auto_stop_at_pct")
    restart = settings.get("restart_at_pct")
    if restart is None:
        restart = threshold
    # Never try to start a full battery or an unplugged car: it can't
    # succeed, and retrying forever would just spam the BYD API.
    can_start = bool(plugged) and not is_charging and pct < 100

    if override_active:
        return ("start", "manual charge-now override active") if can_start else (None, "")
    if not (pct_enabled or time_enabled):
        return ("start", "no limit or window configured") if can_start else (None, "")

    over = pct_enabled and threshold is not None and pct >= float(threshold)
    outside = time_enabled and not in_window
    if is_charging and (over or outside):
        why = []
        if over:
            why.append(f"battery {pct:g}% >= {float(threshold):g}% limit")
        if outside:
            why.append("outside the configured time window")
        return "stop", " and ".join(why)

    if not can_start or (time_enabled and not in_window):
        return None, ""

    # Restart rule. The gap between target and restart exists to stop
    # rapid stop/start cycling. On the FIRST poll inside a freshly
    # opened window the window itself provided that separation, so
    # "under target" is enough; once already inside an ongoing window it
    # doesn't, so the normal gap applies (otherwise standby drain of a
    # single point re-triggers a tiny top-up -- seen live).
    if not pct_enabled:
        return "start", "inside the configured time window"
    just_entered = time_enabled and in_window and not prev_in_window
    if just_entered:
        if threshold is not None and pct < float(threshold):
            return "start", f"under {float(threshold):g}% as the window opened"
    elif restart is not None and pct <= float(restart):
        if time_enabled:
            return "start", f"under {float(restart):g}% and inside the configured time window"
        return "start", f"under {float(restart):g}% restart threshold"
    return None, ""


def plan_next(now: datetime, *, state: dict, settings: dict, ctl: dict,
              pending_action: str | None, override_until: datetime | None,
              standby_rate_fn) -> tuple[datetime, str]:
    """The single answer to 'when should we poll next?'"""
    if pending_action:
        return now + timedelta(minutes=REVIEW_DELAY_MIN), f"reviewing {pending_action} command"

    now_local = now.astimezone(VEHICLE_TZ)
    charging = bool(state.get("is_charging"))
    plugged = bool(state.get("plugged"))
    pct = state.get("battery_pct")
    pct = float(pct) if pct is not None else None
    power = state.get("charging_power_kw")
    power = float(power) if power is not None else None

    pct_enabled = bool(settings.get("pct_limit_enabled"))
    time_enabled = bool(settings.get("time_window_enabled"))
    threshold = settings.get("auto_stop_at_pct")
    restart = settings.get("restart_at_pct")
    raw_interval = settings.get("poll_interval_minutes")
    interval = max(0, int(raw_interval)) if raw_interval is not None else 5
    ws = settings.get("window_start_time") or "00:00"
    we = settings.get("window_end_time") or "06:00"

    cands: list[tuple[datetime, str]] = []

    # Safety-net cadence. interval=0 means "pause idle polling" -- it
    # must NOT also collapse the charging-cadence cap below, or the car
    # would get polled every single minute while charging normally,
    # the opposite of what pausing idle polling is for.
    if charging:
        fast = power is not None and power >= FAST_CHARGE_KW_THRESHOLD
        gap = FAST_CHARGING_INTERVAL_MIN if fast else min(CHARGING_INTERVAL_MIN, interval or CHARGING_INTERVAL_MIN)
        cands.append((now + timedelta(minutes=gap), "fast charging" if fast else "charging"))
    elif interval > 0:
        cands.append((now + timedelta(minutes=interval), "idle interval"))

    if override_until and override_until > now:
        cands.append((override_until, "charge-now override ends"))

    # Phone says the car just parked (settings.burst_until, set by the
    # `parked` edge function from a CarPlay/Bluetooth-disconnect shortcut).
    # A charge starts when someone plugs in, which is minutes after parking,
    # so check often for a short while instead of polling hard all day.
    # Once a plug is seen the burst has done its job: charging has its own
    # cadence, and "plugged but waiting for the window" is handled by the
    # window-open candidate below, so neither needs the burst.
    # "Start charging" was just pressed (settings.fast_until). The car takes
    # seconds to minutes to begin, so check every minute until it is seen
    # charging, so the session is tracked straight away. Charging itself has
    # its own cadence below, so this only applies while NOT yet charging.
    fast_until = settings.get("fast_until")
    if fast_until and fast_until > now and not charging:
        cands.append((now + timedelta(minutes=FAST_GAP_MIN), "starting a charge: checking every minute"))

    burst_until = settings.get("burst_until")
    if burst_until and burst_until > now and not charging and not plugged:
        cands.append((now + timedelta(minutes=BURST_GAP_MIN), "just parked: watching for plug-in"))

    if time_enabled:
        cands.append((next_occurrence(now_local, ws), "window opens"))
        if plugged:  # nothing to stop if it isn't plugged in
            cands.append((next_occurrence(now_local, we), "window closes"))

    if ctl.get("schedule_reset_at"):
        cands.append((max(ctl["schedule_reset_at"], now + MIN_GAP), "restore car schedule"))

    # Predicted moment the battery reaches the target while charging.
    cap = settings.get("battery_capacity_kwh")
    if (charging and pct_enabled and threshold is not None and pct is not None
            and pct < float(threshold) and power and power > 0 and cap):
        per_hour = power / float(cap) * 100.0
        cands.append((now + timedelta(hours=(float(threshold) - pct) / per_hour),
                      f"predicted {float(threshold):g}%"))

    # Predicted moment standby drain reaches the restart threshold.
    if (not charging and plugged and pct_enabled and restart is not None
            and pct is not None and pct > float(restart)
            and (not time_enabled or time_in_window(now_local, ws, we))):
        rate = standby_rate_fn() or DEFAULT_STANDBY_RATE
        if rate > 0:
            cands.append((now + timedelta(hours=(pct - float(restart)) / rate),
                          f"predicted {float(restart):g}%"))

    if not cands:
        # interval=0 with nothing else scheduled (no automation feature
        # enabled, not charging, no override, no pending reset) -- truly
        # nothing to wake up for. Poll once a week as a safety net so a
        # change made elsewhere (e.g. the car plugged in) isn't missed
        # forever, rather than crashing or polling at some tiny default.
        cands.append((now + timedelta(days=7), "paused -- weekly safety check"))

    when, why = min(cands, key=lambda c: c[0])
    floor = now + MIN_GAP
    return (when, why) if when >= floor else (floor, why)


def poll_step(*, now: datetime, state: dict, settings: dict, ctl: dict,
              prev_recorded_at: datetime | None, standby_rate_fn,
              send_command, restore_schedule, notify) -> dict:
    """One poll's decisions. Side effects are injected so production and
    the simulator run identical logic:

      send_command(action) -> bool         start/stop the car
      restore_schedule(start, end) -> bool put the car's own schedule back
      notify(kind, **info)                 push notifications
    """
    now_local = now.astimezone(VEHICLE_TZ)
    time_enabled = bool(settings.get("time_window_enabled"))
    ws = settings.get("window_start_time") or "00:00"
    we = settings.get("window_end_time") or "06:00"
    in_window = bool(time_enabled and time_in_window(now_local, ws, we))
    prev_in_window = bool(time_enabled and prev_recorded_at is not None
                          and time_in_window(prev_recorded_at.astimezone(VEHICLE_TZ), ws, we))

    pct = state.get("battery_pct")
    pct = float(pct) if pct is not None else None
    charging = bool(state.get("is_charging"))

    override_until = settings.get("manual_charge_until")
    override_active = bool(override_until and now < override_until)
    override_expired = bool(override_until and not override_active)

    ctl = dict(ctl)
    action, reason = decide_action(
        is_charging=charging, pct=pct, plugged=bool(state.get("plugged")), settings=settings,
        in_window=in_window, prev_in_window=prev_in_window, override_active=override_active,
    )

    pending = None
    if action:
        last_at = ctl.get("last_command_at")
        recent = bool(ctl.get("last_command") == action and last_at
                      and (now - last_at) < timedelta(minutes=COMMAND_EPISODE_MIN))
        attempts = int(ctl.get("command_attempts") or 0) if recent else 0
        if recent and (now - last_at) < timedelta(minutes=COMMAND_HOLD_MIN):
            pending = action  # sent moments ago; give it time to land
        elif attempts >= COMMAND_MAX_ATTEMPTS:
            if attempts == COMMAND_MAX_ATTEMPTS:  # say so once, then stop nagging
                notify("gave_up", action=action, pct=pct, attempts=attempts)
                ctl["command_attempts"] = attempts + 1
        else:
            ok = send_command(action)
            notify("command", action=action, ok=ok, pct=pct, reason=reason)
            ctl.update(last_command=action, last_command_at=now, command_attempts=attempts + 1)
            if action == "stop":
                reset_at = reset_time_after_stop(now, settings)
                if reset_at is not None:
                    ctl["schedule_reset_at"] = reset_at
            pending = action
    else:
        ctl["command_attempts"] = 0

    # Put the car's own schedule back once it's safe to.
    reset_at = ctl.get("schedule_reset_at")
    if reset_at and now >= reset_at:
        if not time_enabled:
            ctl["schedule_reset_at"] = None
        elif in_window:
            ctl["schedule_reset_at"] = next_occurrence(now_local, we) + timedelta(minutes=RESET_DELAY_MIN)
        elif charging or pending == "stop":
            ctl["schedule_reset_at"] = now + timedelta(minutes=RESET_DELAY_MIN)
        elif restore_schedule(ws, we):
            ctl["schedule_reset_at"] = None
        else:
            notify("schedule_sync_failed", start=ws, end=we)
            ctl["schedule_reset_at"] = now + timedelta(minutes=RESET_RETRY_MIN)

    next_at, why = plan_next(
        now, state=state, settings=settings, ctl=ctl, pending_action=pending,
        override_until=override_until if override_active else None,
        standby_rate_fn=standby_rate_fn,
    )
    return {"ctl": ctl, "next_poll_at": next_at, "reason": why, "clear_override": override_expired}
