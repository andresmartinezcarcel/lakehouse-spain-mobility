"""Bronze: ingest the daily MITMA district-level trip files for a given year.

Source: https://movilidad-opendata.mitma.es (Estudios básicos, por distritos).
Creates:
    bronze_raw_mobility_trips   raw rows exactly as published + ingestion timestamp
    bronze_mobility_data_dates  one row per calendar day with a has_data flag
"""
import argparse
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import requests

from pipeline.db import lakehouse

URL_TEMPLATE = (
    "https://movilidad-opendata.mitma.es/estudios_basicos/por-distritos/viajes/"
    "ficheros-diarios/{ym}/{ymd}_Viajes_distritos.csv.gz"
)


def daily_url(d: date) -> str:
    return URL_TEMPLATE.format(ym=d.strftime("%Y-%m"), ymd=d.strftime("%Y%m%d"))


def url_exists(url: str) -> bool:
    """Check that a file is published without downloading it."""
    try:
        return requests.head(url, timeout=5).status_code == 200
    except requests.RequestException:
        return False


def date_from_url(url: str) -> date:
    ymd = re.search(r"/(\d{8})_Viajes_distritos", url).group(1)
    return datetime.strptime(ymd, "%Y%m%d").date()


def run(year: int, workers: int = 10) -> None:
    days = [date(year, 1, 1) + timedelta(n) for n in range((date(year, 12, 31) - date(year, 1, 1)).days + 1)]
    urls = [daily_url(d) for d in days]

    print(f"Checking {len(urls)} daily files for {year}...")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        valid_urls = [u for u, ok in zip(urls, pool.map(url_exists, urls)) if ok]
    print(f"Found {len(valid_urls)} files, {len(urls) - len(valid_urls)} missing")

    if not valid_urls:
        raise SystemExit("No files found, nothing to ingest.")

    valid_dates = {date_from_url(u) for u in valid_urls}

    with lakehouse() as con:
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze_raw_mobility_trips AS
            SELECT *, CURRENT_TIMESTAMP AS ingestion_date
            FROM read_csv_auto({valid_urls}, compression='gzip', ignore_errors=true)
        """)

        # Calendar of the year with a flag telling whether MITMA published that day
        values = ", ".join(f"('{d}', {str(d in valid_dates).upper()})" for d in days)
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze_mobility_data_dates AS
            SELECT data_date::DATE AS data_date,
                   has_data::BOOLEAN AS has_data,
                   CURRENT_TIMESTAMP AS created_at
            FROM (VALUES {values}) AS t(data_date, has_data)
            ORDER BY data_date
        """)

        print(con.sql("SELECT has_data, COUNT(*) AS days FROM bronze_mobility_data_dates GROUP BY has_data"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2023)
    run(parser.parse_args().year)
