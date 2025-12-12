import duckdb

def run_silver_daily_load(process_date_str: str, db_path='mobility_lake.duckdb'):
    """
    Ingests one day of mobility data.
    Pattern: Idempotent (Delete existing data for this day -> Insert new data).
    
    Args:
        process_date_str (str): The date to process in format 'YYYYMMDD' (e.g., '20231025')
        db_path (str): Path to your DuckDB file.
    """
    
    # 1. ESTABLISH CONNECTION
    con = duckdb.connect()
    attach_query="""ATTACH 'ducklake:my_ducklake.ducklake' AS mobility_ducklake;
    USE mobility_ducklake;"""
    con.execute(attach_query)
    print(f"--- Starting Job for Date: {process_date_str} ---")

    try:
        # 2. ENSURE TARGET TABLE EXISTS (One-time setup)
        # We create it empty if it's not there.
        ddl_query = """
        CREATE OR REPLACE TABLE silver_mobility_trips (
            trip_timestamp TIMESTAMP,
            origin_zone_id VARCHAR,
            destination_zone_id VARCHAR,
            total_trips DOUBLE,
            analysis_id INTEGER,
        );
        """
        con.execute(ddl_query)

        # 3. IDEMPOTENCY STEP: DELETE EXISTING DATA
        # Before we insert, we remove any data that might already exist for this specific date.
        # This prevents duplicates if you re-run the script.
        print(f"Cleaning potential existing data for {process_date_str}...")
        
        delete_query = f"""
        DELETE FROM silver_mobility_trips 
        WHERE strftime(trip_timestamp, '%Y%m%d') = '{process_date_str}';
        """
        con.execute(delete_query)
        
        # 4. DEFINE THE TRANSFORMATION LOGIC (The "Silver" Logic)
        # Note: We filter the Source by the specific process_date_str to only load relevant data
        transformation_sql = f"""
            WITH cleaned_data AS (
                SELECT
                    strptime(t1.date || t1.hour_period, '%Y%m%d%H') AS trip_timestamp,
                    TRIM(t1.origin_zone) AS origin_zone_id,
                    TRIM(t1.destination_zone) AS destination_zone_id,
                    t1.trips AS trip_count,
                    
                FROM
                    bronze_raw_mobility_trips AS t1
                WHERE
                    (t1.origin_zone != 'externo' AND t1.destination_zone != 'externo')
                    AND t1.date= '{process_date_str}'
            )
            SELECT
                trip_timestamp,
                origin_zone_id,
                destination_zone_id,
                -- 4. AGGREGATION
                SUM(trip_count) AS total_trips,
                1 AS analysis_id
            FROM
                cleaned_data
            GROUP BY
                trip_timestamp,
                origin_zone_id,
                destination_zone_id;
        """

        # 5. EXECUTE INGESTION
        print("Transforming and Ingesting new data...")
        
        insert_query = f"""
        INSERT INTO silver_mobility_trips 
        (trip_timestamp, origin_zone_id, destination_zone_id, total_trips, analysis_id)
        {transformation_sql}
        """
        
        con.execute(insert_query)
        
        # 6. VERIFICATION (Optional)
        result_count = con.execute(f"SELECT COUNT(*) FROM silver_mobility_trips WHERE strftime(trip_timestamp, '%Y%m%d') = '{process_date_str}'").fetchone()[0]
        print(f"--- Success! Loaded {result_count} rows for {process_date_str}. ---")

    except Exception as e:
        print(f"!!! Job Failed: {e}")
        raise e
    finally:
        con.close()

# --- ENTRY POINT ---
if __name__ == "__main__":
    # In a real DAG (Airflow/Prefect), this date comes from the context
    import pandas as pd

    for TARGET_DATE in pd.date_range('2023-01-01', '2023-12-31').strftime('%Y%m%d'):
        run_silver_daily_load(TARGET_DATE)