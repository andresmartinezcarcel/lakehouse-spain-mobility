"""Silver: hourly origin-destination trips, loaded one day at a time.

Each daily load is idempotent (delete the day, then insert it again), so any
date range can be re-processed safely, as an orchestrator (Airflow, Prefect...)
would do with one task per day.
"""
import argparse

import pandas as pd

from pipeline.db import lakehouse


def run_silver_daily_load(con, process_date: str) -> int:
    """Load one day (format 'YYYYMMDD') into silver_mobility_trips and return its row count."""
    con.execute(
        "DELETE FROM silver_mobility_trips WHERE strftime(trip_timestamp, '%Y%m%d') = ?",
        [process_date],
    )
    con.execute("""
        INSERT INTO silver_mobility_trips
            (trip_timestamp, origin_zone_id, destination_zone_id, total_trips, analysis_id)
        SELECT
            strptime(CAST(date AS VARCHAR) || hour_period, '%Y%m%d%H') AS trip_timestamp,
            TRIM(origin_zone)      AS origin_zone_id,
            TRIM(destination_zone) AS destination_zone_id,
            SUM(trips)             AS total_trips,
            1                      AS analysis_id
        FROM stg_mobility_trips
        WHERE CAST(date AS VARCHAR) = ?
        GROUP BY ALL
    """, [process_date])
    return con.execute(
        "SELECT COUNT(*) FROM silver_mobility_trips WHERE strftime(trip_timestamp, '%Y%m%d') = ?",
        [process_date],
    ).fetchone()[0]


def run(start: str, end: str) -> None:
    with lakehouse() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS silver_mobility_trips (
                trip_timestamp      TIMESTAMP,
                origin_zone_id      VARCHAR,
                destination_zone_id VARCHAR,
                total_trips         DOUBLE,
                analysis_id         INTEGER
            )
        """)
        for day in pd.date_range(start, end).strftime("%Y%m%d"):
            rows = run_silver_daily_load(con, day)
            print(f"{day}: {rows:,} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="2023-12-31")
    args = parser.parse_args()
    run(args.start, args.end)
