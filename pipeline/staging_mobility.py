"""Staging: apply the data-quality rules found in notebooks/data_quality_check.ipynb.

Reads bronze_raw_mobility_trips (left untouched) and creates stg_mobility_trips:
    - keeps only the columns used downstream, with English names
    - drops outliers: trips more than 10 standard deviations away from the
      mean of their origin-destination pair
    - drops trips to/from Portugal (PT*), France (FR*) and 'externo'
    - strips the '_AM' / '_AD' suffixes so zone codes match INE municipality
      and district codes
"""
from pipeline.db import lakehouse

OUTLIER_STDDEVS = 10


def run() -> None:
    with lakehouse() as con:
        con.execute(f"""
            CREATE OR REPLACE TABLE stg_mobility_trips AS
            WITH renamed AS (
                SELECT
                    fecha   AS date,
                    periodo AS hour_period,
                    origen  AS origin_zone,
                    destino AS destination_zone,
                    viajes  AS trips,
                    ingestion_date
                FROM bronze_raw_mobility_trips
            ),
            od_stats AS (
                SELECT origin_zone, destination_zone,
                       AVG(trips)    AS mean_trips,
                       STDDEV(trips) AS std_trips
                FROM renamed
                GROUP BY origin_zone, destination_zone
            ),
            without_outliers AS (
                SELECT r.*
                FROM renamed r
                JOIN od_stats s USING (origin_zone, destination_zone)
                WHERE s.std_trips IS NULL
                   OR ABS(r.trips - s.mean_trips) <= {OUTLIER_STDDEVS} * s.std_trips
            )
            SELECT
                date,
                hour_period,
                regexp_replace(origin_zone, '_A[MD]$', '')      AS origin_zone,
                regexp_replace(destination_zone, '_A[MD]$', '') AS destination_zone,
                trips,
                ingestion_date
            FROM without_outliers
            WHERE origin_zone <> 'externo' AND destination_zone <> 'externo'
              AND NOT (origin_zone LIKE 'PT%' OR destination_zone LIKE 'PT%')
              AND NOT (origin_zone LIKE 'FR%' OR destination_zone LIKE 'FR%')
        """)

        raw, staged = con.execute("""
            SELECT (SELECT COUNT(*) FROM bronze_raw_mobility_trips),
                   (SELECT COUNT(*) FROM stg_mobility_trips)
        """).fetchone()
        print(f"stg_mobility_trips: {staged:,} rows kept out of {raw:,} ({raw - staged:,} removed)")


if __name__ == "__main__":
    run()
