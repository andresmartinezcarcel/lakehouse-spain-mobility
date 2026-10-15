"""Bronze: load the INE household income atlas (renta media) exported as CSV.

Source: INE, Atlas de distribución de renta de los hogares (ADRH), CSV export
with the columns Municipios; Distritos; Secciones; Indicadores de renta media;
Periodo; Total. Every column is kept as text; parsing happens in silver.
"""
import argparse

from pipeline.db import lakehouse


def run(csv_path: str) -> None:
    with lakehouse() as con:
        con.execute("""
            CREATE OR REPLACE TABLE bronze_economy AS
            SELECT *, CURRENT_TIMESTAMP AS ingestion_date
            FROM read_csv(?, delim=';', header=true, all_varchar=true)
        """, [csv_path])
        rows = con.execute("SELECT COUNT(*) FROM bronze_economy").fetchone()[0]
        print(f"bronze_economy: {rows:,} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", help="Path to the INE CSV file")
    run(parser.parse_args().csv_path)
