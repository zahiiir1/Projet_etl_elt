import duckdb
from pathlib import Path
import time

BASE_DIR = Path(r"C:\Users\asus\Desktop\MLAIM\s2\PBI\Projet_etl_elt\Pipeline_ELT")
DB_PATH = BASE_DIR / "olist_raw.duckdb"
EXPORT_DIR = BASE_DIR / "exports"

EXPORT_DIR.mkdir(exist_ok=True)

tables = [
    "dim_customer",
    "dim_product",
    "dim_seller",
    "dim_date",
    "fact_orders",
    "fact_order_items"
]

start_total = time.time()

conn = duckdb.connect(str(DB_PATH))

print("=" * 70)
print("EXPORT DES TABLES MARTS ELT VERS CSV")
print("=" * 70)

for table in tables:
    start_table = time.time()

    output_path = EXPORT_DIR / f"{table}.csv"
    output_path_sql = str(output_path).replace("\\", "/")

    conn.execute(f"""
        COPY {table}
        TO '{output_path_sql}'
        WITH (HEADER, DELIMITER ',')
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    duration_table = round(time.time() - start_table, 2)

    print(
        f"[OK] {table:<25} -> {count:>10,} lignes "
        f"| temps export: {duration_table}s"
    )

conn.close()

duration_total = round(time.time() - start_total, 2)

print("=" * 70)
print("EXPORT TERMINE")
print(f"Duree totale export : {duration_total}s")
print(f"Dossier export      : {EXPORT_DIR}")
print("=" * 70)