"""Silver: INE census sections (secciones censales) reprojected to WGS84.

Source: INE census section boundaries shapefile (e.g. SECC_CE_20230101.shp),
published in ETRS89 / UTM 30N (EPSG:25830).
"""
import argparse

from pipeline.db import lakehouse


def run(shapefile: str) -> None:
    with lakehouse() as con:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE OR REPLACE TABLE silver_secciones_censales_wgs84 AS
            SELECT
                ST_Transform(geom, 'EPSG:25830', 'EPSG:4326') AS geometria,
                CUSEC AS codigo_seccion_censal,
                CUDIS AS codigo_distrito,
                CUMUN AS codigo_municipio,
                CPRO  AS codigo_provincia,
                CCA   AS codigo_comunidad_autonoma,
                ST_Centroid(geometria) AS centroide
            FROM ST_Read(?)
        """, [shapefile])
        rows = con.execute("SELECT COUNT(*) FROM silver_secciones_censales_wgs84").fetchone()[0]
        print(f"silver_secciones_censales_wgs84: {rows:,} census sections")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shapefile", nargs="?", default="SECC_CE_20230101.shp")
    run(parser.parse_args().shapefile)
