from datetime import datetime, timedelta
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import duckdb
import re

con = duckdb.connect()

def SQL(q):
    """Run SQL (printed for clarity) and return a DataFrame."""
    return con.execute(q).fetchdf()

print("DuckDB version:", con.sql("SELECT version();").fetchone()[0])

SQL("""
    ATTACH 'ducklake:my_ducklake.ducklake' AS mobility_ducklake;
    USE mobility_ducklake;
    """)


def check_url_exists(url):
    """Check if URL exists without downloading the file"""
    try:
        response = requests.head(url, timeout=5)
        return url if response.status_code == 200 else None
    except:
        return None

# Generate all dates for 2023
start = datetime(2023, 1, 1)
end = datetime(2023, 12, 31)
dates = [start + timedelta(days=x) for x in range((end - start).days + 1)]

# Generate all potential URLs
all_urls = [
    f"https://movilidad-opendata.mitma.es/estudios_basicos/por-distritos/viajes/ficheros-diarios/{d.strftime('%Y-%m')}/{d.strftime('%Y%m%d')}_Viajes_distritos.csv.gz" 
    for d in dates
]

print(f"Checking {len(all_urls)} URLs for existence...")

# Check which URLs actually exist (in parallel for speed)
valid_urls = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(check_url_exists, url): url for url in all_urls}
    for i, future in enumerate(as_completed(futures), 1):
        if i % 50 == 0:
            print(f"Checked {i}/{len(all_urls)} URLs...")
        result = future.result()
        if result:
            valid_urls.append(result)

print(f"\nFound {len(valid_urls)} valid URLs out of {len(all_urls)} total dates")
print(f"Missing {len(all_urls) - len(valid_urls)} files")

# Extract dates from valid URLs
def extract_date_from_url(url):
    """Extract date from URL pattern like .../20230115_Viajes_distritos.csv.gz"""
    match = re.search(r'/(\d{8})_Viajes_distritos', url)
    if match:
        date_str = match.group(1)
        return datetime.strptime(date_str, '%Y%m%d').date()
    return None

valid_dates = set(extract_date_from_url(url) for url in valid_urls)
valid_dates.discard(None)

# Now load only the valid URLs
if valid_urls:
    SQL(f"""
    CREATE OR REPLACE TABLE bronze_raw_mobility_trips AS
    SELECT *, CURRENT_TIMESTAMP AS ingestion_date 
    FROM read_csv_auto({valid_urls}, compression='gzip', ignore_errors=true)
    """)
    print("Trips table created successfully!")
    
    # Create table with ALL dates and has_data flag (idempotent)
    all_dates_values = ", ".join([
        f"('{d.date()}', {str(d.date() in valid_dates).upper()})" 
        for d in dates
    ])
    SQL(f"""
    CREATE OR REPLACE TABLE bronze_mobility_data_dates AS
    SELECT 
        data_date::DATE AS data_date,
        has_data::BOOLEAN AS has_data,
        CURRENT_TIMESTAMP AS created_at
    FROM (VALUES {all_dates_values}) AS t(data_date, has_data)
    ORDER BY data_date
    """)
    print(f"Dates table created with {len(dates)} records!")
    
    # Show summary
    print("\nData coverage summary:")
    print(SQL("""
        SELECT 
            has_data,
            COUNT(*) as count 
        FROM bronze_mobility_data_dates 
        GROUP BY has_data
    """))
    
    # Show some missing dates
    print("\nMissing dates sample:")
    print(SQL("SELECT * FROM bronze_mobility_data_dates WHERE has_data = FALSE LIMIT 5"))
    
else:
    print("No valid URLs found!")

SQL("""
    USE memory;
    DETACH mobility_ducklake;
    """)