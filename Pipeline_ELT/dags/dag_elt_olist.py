from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb

PROJECT_DIR = "/opt/airflow/Projet_etl_elt/Pipeline_ELT"

DATA_DIR = f"{PROJECT_DIR}/dataset"
DB_PATH = f"{PROJECT_DIR}/olist_dbt_airflow.duckdb"
EXTRACT_SCRIPT = f"{PROJECT_DIR}/scripts/extract_load.py"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/olist_dbt"
DBT_PROFILES_DIR = f"{PROJECT_DIR}/olist_dbt"

FINAL_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_seller",
    "dim_date",
    "fact_orders",
    "fact_order_items",
]


def validate_elt_tables():
    conn = duckdb.connect(DB_PATH)

    try:
        print("=" * 60)
        print("VALIDATION DES TABLES FINALES ELT")
        print("=" * 60)

        for table in FINAL_TABLES:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table:25s} : {count:,} lignes")

            if count == 0:
                raise ValueError(f"La table {table} est vide")

        print("=" * 60)
        print("VALIDATION ELT TERMINEE AVEC SUCCES")
        print("=" * 60)

    finally:
        conn.close()


default_args = {
    "owner": "master_mlaim",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="elt_olist_duckdb_dbt",
    description="Pipeline ELT Olist avec DuckDB, dbt et Airflow",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["elt", "olist", "duckdb", "dbt"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Demarrage du pipeline ELT Olist'"
    )

    extract_load_duckdb = BashOperator(
        task_id="extract_load_duckdb",
        bash_command=(
            f"DATA_DIR={DATA_DIR} "
            f"DUCKDB_FILE={DB_PATH} "
            f"python {EXTRACT_SCRIPT}"
        )
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt debug --profiles-dir {DBT_PROFILES_DIR} --profile olist_dbt"
        )
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --full-refresh --profiles-dir {DBT_PROFILES_DIR} --profile olist_dbt"
        )
    )

    validate_tables = PythonOperator(
        task_id="validate_elt_tables",
        python_callable=validate_elt_tables
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline ELT termine avec succes'"
    )

    start >> extract_load_duckdb >> dbt_debug >> dbt_run >> validate_tables >> end