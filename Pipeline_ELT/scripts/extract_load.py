"""
extract_load.py
---------------
Pipeline ELT - Phase Extract & Load
Dataset : Brazilian E-Commerce Public Dataset by Olist
Action  : Charge les 8 CSV bruts dans DuckDB SANS transformation
"""

import duckdb
import os
import time

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

# Dossier contenant vos 8 fichiers CSV
DATA_DIR = r"C:\Users\asus\Desktop\MLAIM\s2\PBI\Projet_etl_elt\Pipeline_ELT\dataset"

# Fichier base de données DuckDB (créé automatiquement)
DUCKDB_FILE = r"C:\Users\asus\Desktop\MLAIM\s2\PBI\Projet_etl_elt\Pipeline_ELT\olist_raw.duckdb"

# Les 8 fichiers CSV à charger (sans geolocalisation)
CSV_FILES = {
    "raw_orders":       "olist_orders_dataset.csv",
    "raw_order_items":  "olist_order_items_dataset.csv",
    "raw_order_payments": "olist_order_payments_dataset.csv",
    "raw_order_reviews": "olist_order_reviews_dataset.csv",
    "raw_customers":    "olist_customers_dataset.csv",
    "raw_sellers":      "olist_sellers_dataset.csv",
    "raw_products":     "olist_products_dataset.csv",
    "raw_category_translation": "product_category_name_translation.csv",
}

# ─────────────────────────────────────────
# FONCTIONS
# ─────────────────────────────────────────

def connect_duckdb(db_path: str) -> duckdb.DuckDBPyConnection:
    """Crée ou ouvre la base DuckDB."""
    conn = duckdb.connect(db_path)
    print(f"[OK] Connexion DuckDB : {db_path}")
    return conn


def extract_and_load(conn: duckdb.DuckDBPyConnection, table_name: str, csv_filename: str):
    """
    Charge un CSV brut dans une table DuckDB.
    Aucune transformation — données chargées telles quelles.
    """
    csv_path = os.path.join(DATA_DIR, csv_filename)
    csv_path_sql = csv_path.replace("\\", "/")

    if not os.path.exists(csv_path):
        print(f"[ERREUR] Fichier introuvable : {csv_path}")
        return False

    start = time.time()

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path_sql}', header=True)
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    elapsed = round(time.time() - start, 2)

    print(f"[OK] {table_name:<30} {count:>8} lignes  ({elapsed}s)")
    return True


def show_summary(conn: duckdb.DuckDBPyConnection):
    """Affiche un résumé de toutes les tables chargées."""
    print("\n" + "=" * 55)
    print("  RÉSUMÉ — Tables chargées dans DuckDB")
    print("=" * 55)
    print(f"  {'Table':<30} {'Lignes':>10}  {'Colonnes':>8}")
    print("-" * 55)

    tables = conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()

    for (table,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        cols  = conn.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{table}'").fetchone()[0]
        print(f"  {table:<30} {count:>10}  {cols:>8}")

    print("=" * 55)


def verify_sample(conn: duckdb.DuckDBPyConnection):
    """Affiche un aperçu de la table principale raw_orders."""
    print("\n[APERÇU] raw_orders — 3 premières lignes :")
    print("-" * 55)
    result = conn.execute("SELECT * FROM raw_orders LIMIT 3").fetchdf()
    print(result.to_string(index=False))


# ─────────────────────────────────────────
# EXÉCUTION PRINCIPALE
# ─────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  ELT Pipeline — Extract & Load (Olist → DuckDB)")
    print("=" * 55)

    # 1. Connexion DuckDB
    conn = connect_duckdb(DUCKDB_FILE)

    # 2. Charger chaque CSV brut
    print(f"\n[CHARGEMENT] Source : dossier '{DATA_DIR}/'")
    print("-" * 55)

    success_count = 0
    for table_name, csv_filename in CSV_FILES.items():
        if extract_and_load(conn, table_name, csv_filename):
            success_count += 1

    # 3. Résumé
    show_summary(conn)

    # 4. Aperçu rapide
    verify_sample(conn)

    # 5. Fermeture
    conn.close()
    print(f"\n[TERMINÉ] {success_count}/{len(CSV_FILES)} tables chargées dans '{DUCKDB_FILE}'")
    print("  Prochaine étape : dbt pour les transformations SQL\n")