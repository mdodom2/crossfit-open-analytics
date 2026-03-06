"""
CrossFit Open leaderboard loader -> Snowflake (Bronze)

- Auth: IBM w3ID SSO via external browser
- Loads raw JSON pages into CROSSFIT.BRONZE.OPEN_LEADERBOARD_RAW
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Iterable, Tuple

import requests
import snowflake.connector

# ✅ Clean base URL (no extra text)
C3PO_BASE = "https://c3po.crossfit.com/api/competitions/v2/competitions"


def fetch_pages(
    year: int,
    division: int,
    scaled: int,
    region: int = 0,
    view: int = 0,
    sort: int = 0,
    sleep_s: float = 0.25,
) -> Iterable[Tuple[int, Dict[str, Any]]]:
    """
    Yield (page_num, payload_json) for CrossFit Open leaderboard pages.
    """
    url = f"{C3PO_BASE}/open/{year}/leaderboards"
    params = {
        "view": view,
        "division": division,
        "region": region,
        "scaled": scaled,
        "sort": sort,
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    first = r.json()

    total_pages = int(first.get("pagination", {}).get("totalPages", 1))
    yield 1, first

    for page in range(2, total_pages + 1):
        params["page"] = page
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        yield page, r.json()
        time.sleep(sleep_s)


def test_snowflake_connection(conn: snowflake.connector.SnowflakeConnection) -> None:
    """
    Quick sanity check that the Snowflake connection works and shows the active context.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              CURRENT_USER(),
              CURRENT_ROLE(),
              CURRENT_WAREHOUSE(),
              CURRENT_DATABASE(),
              CURRENT_SCHEMA()
            """
        )
        print("✅ Snowflake connection OK:", cur.fetchone())


def load_to_snowflake(
    conn: snowflake.connector.SnowflakeConnection,
    year: int,
    division: int,
    scaled: int,
    region: int = 0,
    view: int = 0,
    sort: int = 0,
) -> None:
    insert_sql = """
        INSERT INTO CROSSFIT.BRONZE.OPEN_LEADERBOARD_RAW (year, division, scaled, region, page, payload)
        SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s)
    """

    with conn.cursor() as cur:
        for page_num, payload in fetch_pages(
            year=year,
            division=division,
            scaled=scaled,
            region=region,
            view=view,
            sort=sort,
        ):
            cur.execute(
                insert_sql,
                (year, division, scaled, region, page_num, json.dumps(payload)),
            )

            # progress indicator
            if page_num == 1 or page_num % 10 == 0:
                total_pages = payload.get("pagination", {}).get("totalPages", "?")
                print(
                    f"loaded year={year} division={division} scaled={scaled} "
                    f"page={page_num}/{total_pages}"
                )


if __name__ == "__main__":
    # --- Snowflake connection settings (IBM w3ID SSO) ---
    SF_ACCOUNT = "XQEFMFM-HAKKODAINC_PARTNER"
    SF_USER = "MATTHEW.ODOM@IBM.COM"
    SF_WAREHOUSE = "COMPUTE_WH"
    SF_ROLE = "SYSADMIN"
    SF_DATABASE = "CROSSFIT"
    SF_SCHEMA = "BRONZE"

    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        authenticator="externalbrowser",  # ✅ SSO via browser (w3ID)
    )

    try:
        # ✅ Run this first to confirm auth/context works before pulling lots of data
        test_snowflake_connection(conn)

        # --- Data pull settings ---
        # Tip: start small while testing, then expand.
        years = [2025]          # expand later to [2023, 2024, 2025]
        divisions = [1]         # expand later to [1, 2, 18]
        scaled_vals = [0]       # expand later to [0, 1]

        for y in years:
            for d in divisions:
                for s in scaled_vals:
                    print(f"Starting load: year={y}, division={d}, scaled={s}")
                    load_to_snowflake(conn, year=y, division=d, scaled=s, region=0)

    finally:
        conn.close()