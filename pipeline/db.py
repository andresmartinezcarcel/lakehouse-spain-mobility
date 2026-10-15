"""Shared connection helper for the lakehouse."""
import os
from contextlib import contextmanager

import duckdb

# Catalog to attach. Defaults to a local DuckLake catalog; point it to a plain
# DuckDB file (e.g. LAKEHOUSE_URI=dev.duckdb) for quick local experiments.
LAKEHOUSE_URI = os.getenv("LAKEHOUSE_URI", "ducklake:my_ducklake.ducklake")
LAKEHOUSE_ALIAS = "mobility_ducklake"


@contextmanager
def lakehouse():
    """Yield a DuckDB connection with the lakehouse attached and selected."""
    con = duckdb.connect()
    con.execute(f"ATTACH '{LAKEHOUSE_URI}' AS {LAKEHOUSE_ALIAS}")
    con.execute(f"USE {LAKEHOUSE_ALIAS}")
    try:
        yield con
    finally:
        con.close()
