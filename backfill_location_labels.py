"""One-off backfill: compute location_label for existing charging_sessions
rows that already have coordinates but no label (sessions recorded before
the geocoding feature shipped). Coordinates are fixed facts -- the suburb
a given lat/lng sits in doesn't depend on when we looked it up, unlike
cost, which genuinely depended on the rate rules in effect at the time.
That's the distinction that makes backfilling this safe where backfilling
cost wouldn't have been.

Respects Nominatim's 1-request/second usage policy with an explicit delay
between calls, same User-Agent requirement as the live poll.py path.

Meant to be run once, via a GitHub Actions job (this needs real internet
access to Nominatim, which this sandbox doesn't have), not left as a
standing part of the app.
"""

from __future__ import annotations

import os
import time

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
HEADERS = {
    "apikey": os.environ["SUPABASE_SERVICE_KEY"],
    "Authorization": f"Bearer {os.environ['SUPABASE_SERVICE_KEY']}",
    "Content-Type": "application/json",
}


def reverse_geocode(lat: float | None, lon: float | None) -> str | None:
    if lat is None or lon is None:
        return None
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lon, "format": "jsonv2", "zoom": 14, "addressdetails": 1},
            headers={"User-Agent": "byd-atto3-charging-tracker/1.0 (personal project)"},
            timeout=10,
        )
        resp.raise_for_status()
        addr = resp.json().get("address", {})
        locality = addr.get("suburb") or addr.get("town") or addr.get("city") or addr.get("village")
        state = addr.get("state")
        au_state_abbrev = {
            "New South Wales": "NSW", "Victoria": "VIC", "Queensland": "QLD",
            "Western Australia": "WA", "South Australia": "SA", "Tasmania": "TAS",
            "Australian Capital Territory": "ACT", "Northern Territory": "NT",
        }
        state = au_state_abbrev.get(state, state)
        if locality and state:
            return f"{locality}, {state}"
        return locality or state
    except Exception as exc:
        print(f"reverse_geocode failed for ({lat}, {lon}): {exc}")
        return None


def main() -> None:
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/charging_sessions",
        headers=HEADERS,
        params={
            "select": "id,start_latitude,start_longitude",
            "location_label": "is.null",
            "start_latitude": "not.is.null",
        },
        timeout=15,
    )
    resp.raise_for_status()
    rows = resp.json()
    print(f"{len(rows)} session(s) need a location_label")

    for i, row in enumerate(rows):
        label = reverse_geocode(row["start_latitude"], row["start_longitude"])
        print(f"session {row['id']}: ({row['start_latitude']}, {row['start_longitude']}) -> {label!r}")
        if label is not None:
            patch = requests.patch(
                f"{SUPABASE_URL}/rest/v1/charging_sessions",
                headers=HEADERS,
                params={"id": f"eq.{row['id']}"},
                json={"location_label": label},
                timeout=15,
            )
            patch.raise_for_status()
        # Nominatim usage policy: max 1 request/second. Only sleep between
        # calls, not after the last one.
        if i < len(rows) - 1:
            time.sleep(1.1)

    print("done")


if __name__ == "__main__":
    main()
