import duckdb

con = duckdb.connect()
def SQL(q):
    """Run SQL (printed for clarity) and return a DataFrame."""
    return con.execute(q).fetchdf()
print("DuckDB version:", con.sql("SELECT version();").fetchone()[0])
SQL("""
    ATTACH 'ducklake:my_ducklake.ducklake' AS mobility_ducklake;
    USE mobility_ducklake;
    """)
SQL("LOAD spatial;")
SQL("""
CREATE OR REPLACE TABLE silver_secciones_censales_wgs84 AS
SELECT 
    ST_Transform(geom, 'EPSG:25830', 'EPSG:4326') AS geometria,
    CUSEC AS codigo_seccion_censal,
    CUDIS AS codigo_distrito,
    CUMUN AS codigo_municipio,
    CPRO AS codigo_provincia,
    CCA AS codigo_comunidad_autonoma,
    ST_Centroid(geometria) AS centroide
FROM ST_Read('SECC_CE_20230101.shp')
""")

SQL("""
    USE memory;
    DETACH mobility_ducklake;
    """)