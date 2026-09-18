# 2023 BYD Atto 3 — Charging Cost Tracker

Polls the BYD vehicle API every 15 minutes via [pyBYD](https://github.com/jkaberg/pyBYD),
logs telemetry to Supabase, and automatically opens/closes a charging session
whenever it detects the car has started or stopped charging — so every charge
you do gets its own record of: km driven since the last charge, % added, kWh
added, and cost.

## How it works

- `poll.py` runs once per invocation (not a long-running process). It:
  1. Logs into the BYD API and fetches battery %, charging state, odometer, GPS.
  2. Writes a row to `vehicle_snapshots` every time, regardless of state.
  3. Compares the current charging state to the previous poll's state:
     - **not charging → charging**: opens a new row in `charging_sessions`.
     - **charging → not charging**: closes the open session, computing
       `energy_added_kwh` (from % delta × battery capacity), `cost` (auto via
       a generated column), and `km_since_last_charge` (from the odometer at
       the start of this session vs. the end of the last closed one).
- `.github/workflows/poll.yml` runs `poll.py` on a schedule via GitHub Actions
  (free, no server to maintain).

## Setup

1. **GitHub repo secrets** (Settings → Secrets and variables → Actions):
   - `BYD_USERNAME`, `BYD_PASSWORD` — ideally a dedicated BYD account, not your main one
   - `BYD_COUNTRY_CODE` — `AU`
   - `BYD_CONTROL_PIN` — optional, only needed for remote commands (not used by this poller)
   - `SUPABASE_URL` — `https://ujpcabmacpnbtyiharnk.supabase.co`
   - `SUPABASE_SERVICE_KEY` — the `service_role` key from Supabase dashboard →
     Project Settings → API. **Not** the anon/publishable key — this script
     needs write access, which only the service key grants.

2. Once secrets are set, the workflow runs automatically every 15 minutes.
   You can also trigger it manually from the Actions tab (`workflow_dispatch`).

## Local testing

```bash
cp .env.example .env   # fill in real values
pip install -r requirements.txt
set -a && source .env && set +a
python poll.py
```

## Notes / open items

- `charging_power_kw` is populated from a field that isn't fully confirmed
  against a live Atto 3 yet — treat it as informational. Cost math relies
  only on battery % delta, so it's unaffected.
- Battery capacity and electricity rate are stored in the `tracker_settings`
  table in Supabase (single settings row) — update there, not in code.
- Uses a dedicated BYD account is recommended so you don't get logged out of
  the BYD app on your phone.
