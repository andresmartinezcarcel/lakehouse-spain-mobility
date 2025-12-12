import duckdb

def run_silver_economy_load():
    """
    Load economy data from bronze_economy into Silver table silver_economy_aggregated,
    aggregated by municipality, district, section and year.

    bronze_economy columns (all TEXT except "Periodo" may be BIGINT):
        - Municipios
        - Distritos
        - Secciones
        - Indicadores de renta media  (text label, NOT numeric)
        - Periodo                     (text or BIGINT)
        - Total                       (numeric value as text)

    Target table:
        silver_economy_aggregated(
            municipality_code VARCHAR,
            district_code     VARCHAR,
            section_code      VARCHAR,
            year              INTEGER,
            avg_income        DOUBLE
        )
    """

    # 1. CONNECT & ATTACH DUCKLAKE
    con = duckdb.connect()
    con.execute("""
        ATTACH 'ducklake:my_ducklake.ducklake' AS mobility_ducklake;
        USE mobility_ducklake;
    """)
    print("--- Starting Economy Job (ALL YEARS) ---")

    try:
        # 2. CREATE SILVER TABLE IF NOT EXISTS
        con.execute("""
        CREATE TABLE IF NOT EXISTS silver_economy_aggregated (
            municipality_code VARCHAR,
            district_code VARCHAR,
            section_code VARCHAR,
            year INTEGER,
            avg_income DOUBLE
        );
        """)

        # 3. CLEAR SILVER (FULL RELOAD)
        print("Cleaning existing data from silver_economy_aggregated...")
        con.execute("DELETE FROM silver_economy_aggregated;")

        # 4. DEBUG: BRONZE ROW COUNT
        total_rows = con.execute("SELECT COUNT(*) FROM bronze_economy;").fetchone()[0]
        print(f"Rows in bronze_economy: {total_rows}")

        # 5. DEBUG: COUNT ROWS WITH NON-EMPTY TOTAL
        total_non_empty = con.execute("""
            SELECT COUNT(*)
            FROM bronze_economy
            WHERE TRIM("Total") <> '';
        """).fetchone()[0]
        print(f"Rows with non-empty TOTAL: {total_non_empty}")

        # 6. DEBUG: ROWS THAT PARSE AS NUMERIC
        total_numeric = con.execute("""
            WITH parsed AS (
                SELECT
                    TRY_CAST(
                        REPLACE(
                            REPLACE(TRIM("Total"), '.', ''),
                            ',', '.'
                        ) AS DOUBLE
                    ) AS val
                FROM bronze_economy
            )
            SELECT COUNT(*) FROM parsed WHERE val IS NOT NULL;
        """).fetchone()[0]
        print(f"Rows where TOTAL parses to a valid number: {total_numeric}")

        # 7. MAIN TRANSFORMATION SQL
        transformation_sql = """
            WITH cleaned AS (
                SELECT
                    -- Extract codes
                    split_part("Municipios", ' ', 1) AS municipality_code,
                    split_part("Distritos", ' ', 1) AS district_code,
                    split_part("Secciones", ' ', 1) AS section_code,

                    -- FIX: Periodo may be BIGINT -> cast to varchar first
                    CAST(right(CAST("Periodo" AS VARCHAR), 4) AS INTEGER) AS year,

                    -- Raw income text (numeric stored as text)
                    NULLIF(TRIM("Total"), '') AS raw_income
                FROM bronze_economy
            ),
            parsed AS (
                SELECT
                    municipality_code,
                    district_code,
                    section_code,
                    year,
                    TRY_CAST(
                        REPLACE(
                            REPLACE(raw_income, '.', ''),  -- remove thousand separators
                            ',', '.'                       -- convert decimal comma to dot
                        ) AS DOUBLE
                    ) AS income_value
                FROM cleaned
            )
            SELECT
                municipality_code,
                district_code,
                section_code,
                year,
                AVG(income_value) AS avg_income
            FROM parsed
            WHERE
                income_value IS NOT NULL
                AND municipality_code IS NOT NULL
                AND district_code IS NOT NULL
                AND section_code IS NOT NULL
            GROUP BY
                municipality_code,
                district_code,
                section_code,
                year
        """

        print("Inserting aggregated data into silver_economy_aggregated...")

        con.execute(f"""
            INSERT INTO silver_economy_aggregated
            (municipality_code, district_code, section_code, year, avg_income)
            {transformation_sql};
        """)

        # 8. FINAL COUNT
        result = con.execute("SELECT COUNT(*) FROM silver_economy_aggregated;").fetchone()[0]
        print(f"--- SUCCESS! Loaded {result} aggregated rows into silver_economy_aggregated. ---")

    except Exception as e:
        print("!!! ERROR during economy aggregation !!!")
        print(e)
        raise
        


# ENTRY POINT
if __name__ == "__main__":
    run_silver_economy_load()


    con = duckdb.connect()
    con.execute("""
        ATTACH 'ducklake:my_ducklake.ducklake' AS mobility_ducklake;
        USE mobility_ducklake;
    """)
    print(con.execute("""SELECT * FROM silver_economy_aggregated LIMIT 100;""").fetch_df())
    print(con.execute("""
    SELECT *, COUNT(*) as duplicate_count
    FROM silver_economy_aggregated
    GROUP BY ALL
    HAVING COUNT(*) > 1
    """).fetch_df())
    print(con.execute("""
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN municipality_code IS NULL THEN 1 ELSE 0 END) as municipality_code_nulls,
        SUM(CASE WHEN district_code IS NULL THEN 1 ELSE 0 END) as district_code_nulls,
        SUM(CASE WHEN section_code IS NULL THEN 1 ELSE 0 END) as section_code_nulls,
        SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) as year_nulls,
        SUM(CASE WHEN avg_income IS NULL THEN 1 ELSE 0 END) as avg_income_nulls
    FROM silver_economy_aggregated
    """).fetch_df())
    con.close()