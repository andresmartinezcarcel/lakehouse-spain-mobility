"""Gold: hourly mobility profile by type of day.

Classifies every day as a weekday (Monday..Friday), weekend, national holiday
or bridge day ("puente": a Friday after a Thursday holiday or a Monday before
a Tuesday holiday) and computes, for each type, the share of the day's trips
made in each hour.
"""
from pipeline.db import lakehouse

NATIONAL_HOLIDAYS_2023 = [
    ("2023-01-06", "Epifanía del Señor"),
    ("2023-04-07", "Viernes Santo"),
    ("2023-05-01", "Fiesta del Trabajo"),
    ("2023-08-15", "Asunción de la Virgen"),
    ("2023-10-12", "Fiesta Nacional de España"),
    ("2023-11-01", "Todos los Santos"),
    ("2023-12-06", "Día de la Constitución Española"),
    ("2023-12-08", "Inmaculada Concepción"),
    ("2023-12-25", "Natividad del Señor"),
]


def run() -> None:
    with lakehouse() as con:
        values = ", ".join(f"(DATE '{d}', '{name}')" for d, name in NATIONAL_HOLIDAYS_2023)
        con.execute(f"""
            CREATE OR REPLACE TABLE ref_national_holidays AS
            SELECT * FROM (VALUES {values}) AS t(holiday_date, holiday_name)
        """)

        con.execute("""
            CREATE OR REPLACE TABLE gold_hourly_trip_profile AS
            WITH hourly AS (
                SELECT
                    CAST(trip_timestamp AS DATE) AS day,
                    HOUR(trip_timestamp)         AS hour,
                    SUM(total_trips)             AS trips
                FROM silver_mobility_trips
                GROUP BY ALL
            ),
            classified AS (
                SELECT
                    h.*,
                    CASE
                        WHEN h.day IN (SELECT holiday_date FROM ref_national_holidays) THEN 'Holiday'
                        WHEN ISODOW(h.day) IN (6, 7) THEN 'Weekend'
                        WHEN ISODOW(h.day) = 5
                         AND h.day - 1 IN (SELECT holiday_date FROM ref_national_holidays) THEN 'Bridge day'
                        WHEN ISODOW(h.day) = 1
                         AND h.day + 1 IN (SELECT holiday_date FROM ref_national_holidays) THEN 'Bridge day'
                        ELSE dayname(h.day)
                    END AS day_type
                FROM hourly h
            )
            SELECT
                day_type,
                hour,
                SUM(trips) AS total_trips,
                ROUND(100.0 * SUM(trips) / SUM(SUM(trips)) OVER (PARTITION BY day_type), 2) AS pct_of_day
            FROM classified
            GROUP BY day_type, hour
            ORDER BY day_type, hour
        """)
        print(con.sql("""
            SELECT day_type, arg_max(hour, pct_of_day) AS peak_hour, max(pct_of_day) AS peak_pct
            FROM gold_hourly_trip_profile GROUP BY day_type ORDER BY day_type
        """))


if __name__ == "__main__":
    run()
