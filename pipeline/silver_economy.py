"""Silver: average household income per census section and year.

Reads bronze_economy (INE text export) and creates silver_economy_aggregated:
    - extracts the municipality / district / section codes from their labels
      (e.g. '28079 Madrid' -> '28079')
    - parses Spanish-formatted numbers ('12.345,6' -> 12345.6)
    - averages the income indicators per section and year
"""
from pipeline.db import lakehouse


def run() -> None:
    with lakehouse() as con:
        con.execute("""
            CREATE OR REPLACE TABLE silver_economy_aggregated AS
            WITH cleaned AS (
                SELECT
                    split_part("Municipios", ' ', 1) AS municipality_code,
                    split_part("Distritos", ' ', 1)  AS district_code,
                    split_part("Secciones", ' ', 1)  AS section_code,
                    -- Periodo may be read as BIGINT or text: keep the last 4 chars (year)
                    CAST(right(CAST("Periodo" AS VARCHAR), 4) AS INTEGER) AS year,
                    TRY_CAST(
                        REPLACE(REPLACE(NULLIF(TRIM("Total"), ''), '.', ''), ',', '.')
                        AS DOUBLE
                    ) AS income_value
                FROM bronze_economy
            )
            SELECT
                municipality_code,
                district_code,
                section_code,
                year,
                AVG(income_value) AS avg_income
            FROM cleaned
            WHERE income_value IS NOT NULL
              AND municipality_code <> ''
              AND district_code <> ''
              AND section_code <> ''
            GROUP BY municipality_code, district_code, section_code, year
        """)

        bronze, parsed, silver = con.execute("""
            SELECT
                (SELECT COUNT(*) FROM bronze_economy),
                (SELECT COUNT(*) FROM bronze_economy
                 WHERE TRY_CAST(REPLACE(REPLACE(TRIM("Total"), '.', ''), ',', '.') AS DOUBLE) IS NOT NULL),
                (SELECT COUNT(*) FROM silver_economy_aggregated)
        """).fetchone()
        print(f"bronze_economy: {bronze:,} rows, {parsed:,} with a numeric value")
        print(f"silver_economy_aggregated: {silver:,} section-year rows")


if __name__ == "__main__":
    run()
