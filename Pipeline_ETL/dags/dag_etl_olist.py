from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import time
import os

# ============================================================
# CONFIGURATION GLOBALE
# ============================================================
#DAG
DATA_PATH = "/opt/airflow/dataset"
def get_db_url():
    """Build the DB connection URL at task runtime, not at import time."""
    db_user     = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]
    db_host     = os.environ["DB_HOST"]
    db_port     = os.environ["DB_PORT"]
    db_name     = os.environ["DB_NAME"]
    return f"postgresql://{db_user}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}"

default_args = {
    "owner"          : "master_mlaim",
    "retries"        : 2,
    "retry_delay"    : timedelta(minutes=3),
    "email_on_failure": False,
}

# ============================================================
# TÂCHE 1 : EXTRACT
# ============================================================
def task_extract(**kwargs):
    start = time.time()
    print("[EXTRACT] Lecture des fichiers CSV...")

    orders = pd.read_csv(f"{DATA_PATH}/olist_orders_dataset.csv")
    customers = pd.read_csv(f"{DATA_PATH}/olist_customers_dataset.csv")
    items = pd.read_csv(f"{DATA_PATH}/olist_order_items_dataset.csv")
    products = pd.read_csv(f"{DATA_PATH}/olist_products_dataset.csv")
    sellers = pd.read_csv(f"{DATA_PATH}/olist_sellers_dataset.csv")
    payments = pd.read_csv(f"{DATA_PATH}/olist_order_payments_dataset.csv")
    reviews = pd.read_csv(f"{DATA_PATH}/olist_order_reviews_dataset.csv")
    translation = pd.read_csv(f"{DATA_PATH}/product_category_name_translation.csv")

    # Sauvegarder pour la tâche suivante via XCom
    orders.to_parquet("/tmp/etl_orders.parquet")
    customers.to_parquet("/tmp/etl_customers.parquet")
    items.to_parquet("/tmp/etl_items.parquet")
    products.to_parquet("/tmp/etl_products.parquet")
    sellers.to_parquet("/tmp/etl_sellers.parquet")
    payments.to_parquet("/tmp/etl_payments.parquet")
    reviews.to_parquet("/tmp/etl_reviews.parquet")
    translation.to_parquet("/tmp/etl_translation.parquet")

    print(f"[EXTRACT] Terminé en {round(time.time()-start, 2)}s")
    print(f"  orders     : {len(orders):,} lignes")
    print(f"  customers  : {len(customers):,} lignes")
    print(f"  items      : {len(items):,} lignes")

# ============================================================
# TÂCHE 2 : TRANSFORM
# ============================================================
def task_transform(**kwargs):
    start = time.time()
    print("[TRANSFORM] Chargement des données extraites...")

    orders      = pd.read_parquet("/tmp/etl_orders.parquet")
    customers   = pd.read_parquet("/tmp/etl_customers.parquet")
    items       = pd.read_parquet("/tmp/etl_items.parquet")
    products    = pd.read_parquet("/tmp/etl_products.parquet")
    sellers     = pd.read_parquet("/tmp/etl_sellers.parquet")
    payments    = pd.read_parquet("/tmp/etl_payments.parquet")
    reviews     = pd.read_parquet("/tmp/etl_reviews.parquet")
    translation = pd.read_parquet("/tmp/etl_translation.parquet")

    # --- NETTOYAGE ---
    date_cols = [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in date_cols:
        orders[col] = pd.to_datetime(orders[col], errors="coerce")
    items["shipping_limit_date"] = pd.to_datetime(
        items["shipping_limit_date"], errors="coerce"
    )
    products = products.merge(translation, on="product_category_name", how="left")
    reviews["review_comment_title"]   = reviews["review_comment_title"].fillna("")
    reviews["review_comment_message"] = reviews["review_comment_message"].fillna("")
    products["product_category_name"] = products["product_category_name"].fillna("unknown")
    products["product_category_name_english"] = products["product_category_name_english"].fillna("unknown")
    for col in ["product_weight_g","product_length_cm","product_height_cm","product_width_cm","product_photos_qty"]:
        products[col] = products[col].fillna(0)
    customers["customer_city"]  = customers["customer_city"].str.strip().str.lower()
    customers["customer_state"] = customers["customer_state"].str.strip().str.upper()
    sellers["seller_city"]      = sellers["seller_city"].str.strip().str.lower()
    sellers["seller_state"]     = sellers["seller_state"].str.strip().str.upper()
    products["product_category_name_english"] = products["product_category_name_english"].str.strip().str.lower()
    items["price"]            = items["price"].round(2)
    items["freight_value"]    = items["freight_value"].round(2)
    payments["payment_value"] = payments["payment_value"].round(2)

    # --- ENRICHISSEMENT ---
    payments_agg = payments.groupby("order_id").agg(
        montant_total = ("payment_value", "sum"),
        nb_paiements  = ("payment_sequential", "max"),
        nb_versements = ("payment_installments", "max")
    ).reset_index()
    main_payment = (
        payments.sort_values("payment_value", ascending=False)
        .drop_duplicates("order_id")[["order_id","payment_type"]]
        .rename(columns={"payment_type": "type_paiement"})
    )
    payments_agg = payments_agg.merge(main_payment, on="order_id", how="left")
    payments_agg["montant_total"] = payments_agg["montant_total"].round(2)

    orders["delai_livraison_jours"] = (
        orders["order_delivered_customer_date"] -
        orders["order_purchase_timestamp"]
    ).dt.days
    orders["livraison_en_avance"] = (
        orders["order_estimated_delivery_date"] -
        orders["order_delivered_customer_date"]
    ).dt.days
    orders["annee_achat"]        = orders["order_purchase_timestamp"].dt.year
    orders["mois_achat"]         = orders["order_purchase_timestamp"].dt.month
    orders["trimestre_achat"]    = orders["order_purchase_timestamp"].dt.quarter
    orders["jour_semaine_achat"] = orders["order_purchase_timestamp"].dt.day_name()

    items["montant_total_article"] = (items["price"] + items["freight_value"]).round(2)
    items["part_frais_livraison"]  = (
        items["freight_value"] /
        items["montant_total_article"].replace(0, pd.NA) * 100
    ).round(2)

    def tranche_prix(price):
        if price < 50:    return "cheap"
        elif price < 200: return "medium"
        else:             return "premium"
    items["tranche_prix"] = items["price"].apply(tranche_prix)

    def statut_livraison(jours):
        if pd.isna(jours): return "non livre"
        elif jours > 0:    return "en avance"
        elif jours == 0:   return "a temps"
        else:              return "en retard"
    orders["statut_livraison"] = orders["livraison_en_avance"].apply(statut_livraison)

    orders["livraison_rapide"] = orders["delai_livraison_jours"].apply(
        lambda x: 1 if pd.notna(x) and x < 7 else 0
    ).astype("Int64")

    def classer_satisfaction(score):
        if pd.isna(score): return "inconnu"
        elif score >= 4:   return "positif"
        elif score == 3:   return "neutre"
        else:              return "negatif"

    reviews["satisfaction"] = reviews["review_score"].apply(classer_satisfaction)
    reviews["a_commente"]   = (reviews["review_comment_message"].str.strip() != "").astype(int)

    reviews_agg = reviews.groupby("order_id").agg(
        review_score = ("review_score", "mean"),
        nb_reviews   = ("review_id", "count"),
        a_commente   = ("a_commente", "max")
    ).reset_index()
    reviews_agg["review_score"] = reviews_agg["review_score"].round(2)
    reviews_agg["satisfaction"] = reviews_agg["review_score"].apply(classer_satisfaction)

    products["volume_cm3"] = (
        products["product_length_cm"] *
        products["product_height_cm"] *
        products["product_width_cm"]
    ).round(2)

    # --- SCHÉMA EN ÉTOILE ---
    dim_customer = customers[[
        "customer_id","customer_unique_id",
        "customer_zip_code_prefix","customer_city","customer_state"
    ]].drop_duplicates(subset=["customer_id"]).rename(columns={
        "customer_zip_code_prefix":"zip_code_prefix",
        "customer_city":"city","customer_state":"state"
    })

    dim_product = products[[
        "product_id","product_category_name_english",
        "product_weight_g","product_length_cm",
        "product_height_cm","product_width_cm","volume_cm3"
    ]].drop_duplicates(subset=["product_id"]).rename(columns={
        "product_category_name_english":"category",
        "product_weight_g":"weight_g","product_length_cm":"length_cm",
        "product_height_cm":"height_cm","product_width_cm":"width_cm"
    })

    dim_seller = sellers[[
        "seller_id","seller_zip_code_prefix",
        "seller_city","seller_state"
    ]].drop_duplicates(subset=["seller_id"]).rename(columns={
        "seller_zip_code_prefix":"zip_code_prefix",
        "seller_city":"city","seller_state":"state"
    })

    toutes_dates = orders["order_purchase_timestamp"].dropna()
    dim_date = pd.DataFrame({"date": toutes_dates.dt.date.unique()})
    dim_date["date"]        = pd.to_datetime(dim_date["date"])
    dim_date["date_id"]     = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["day"]         = dim_date["date"].dt.day
    dim_date["month"]       = dim_date["date"].dt.month
    dim_date["month_name"]  = dim_date["date"].dt.month_name()
    dim_date["quarter"]     = dim_date["date"].dt.quarter
    dim_date["year"]        = dim_date["date"].dt.year
    dim_date["day_of_week"] = dim_date["date"].dt.day_name()
    dim_date["week_number"] = dim_date["date"].dt.isocalendar().week.astype(int)
    dim_date["is_weekend"]  = dim_date["date"].dt.dayofweek.isin([5,6]).astype(int)
    dim_date = dim_date.drop_duplicates(subset=["date_id"])

    faits = orders.merge(customers[["customer_id"]], on="customer_id", how="left")
    faits = faits.merge(payments_agg, on="order_id", how="left")
    faits = faits.merge(reviews_agg, on="order_id", how="left")
    faits["date_id"] = faits["order_purchase_timestamp"].dt.strftime("%Y%m%d").astype("Int64")

    fact_orders = faits[[
        "order_id","customer_id","date_id","order_status",
        "montant_total","nb_versements","type_paiement","nb_paiements",
        "delai_livraison_jours","livraison_en_avance",
        "statut_livraison","livraison_rapide",
        "annee_achat","mois_achat","trimestre_achat","jour_semaine_achat",
        "review_score","satisfaction","a_commente","nb_reviews"
    ]].rename(columns={"order_status":"status"})

    fact_order_items = items[[
        "order_id","order_item_id","product_id","seller_id",
        "shipping_limit_date","price","freight_value",
        "montant_total_article","part_frais_livraison","tranche_prix"
    ]].rename(columns={
        "montant_total_article":"total_amount",
        "part_frais_livraison":"freight_pct",
        "tranche_prix":"price_range"
    })

    # Sauvegarder pour la tâche Load
    dim_date.to_parquet("/tmp/etl_dim_date.parquet")
    dim_customer.to_parquet("/tmp/etl_dim_customer.parquet")
    dim_product.to_parquet("/tmp/etl_dim_product.parquet")
    dim_seller.to_parquet("/tmp/etl_dim_seller.parquet")
    fact_orders.to_parquet("/tmp/etl_fact_orders.parquet")
    fact_order_items.to_parquet("/tmp/etl_fact_order_items.parquet")

    print(f"[TRANSFORM] Terminé en {round(time.time()-start, 2)}s")
    print(f"  fact_orders      : {len(fact_orders):,} lignes")
    print(f"  fact_order_items : {len(fact_order_items):,} lignes")

# ============================================================
# TÂCHE 3 : LOAD
# ============================================================
def task_load(**kwargs):
    start = time.time()
    print("[LOAD] Chargement dans PostgreSQL...")

    engine = create_engine(get_db_url())  # URL built at runtime

    dim_date         = pd.read_parquet("/tmp/etl_dim_date.parquet")
    dim_customer     = pd.read_parquet("/tmp/etl_dim_customer.parquet")
    dim_product      = pd.read_parquet("/tmp/etl_dim_product.parquet")
    dim_seller       = pd.read_parquet("/tmp/etl_dim_seller.parquet")
    fact_orders      = pd.read_parquet("/tmp/etl_fact_orders.parquet")
    fact_order_items = pd.read_parquet("/tmp/etl_fact_order_items.parquet")

    dim_date.to_sql("dim_date", engine, if_exists="replace", index=False)
    dim_customer.to_sql("dim_customer", engine, if_exists="replace", index=False)
    dim_product.to_sql("dim_product", engine, if_exists="replace", index=False)
    dim_seller.to_sql("dim_seller", engine, if_exists="replace", index=False)
    fact_orders.to_sql("fact_orders", engine, if_exists="replace", index=False, chunksize=5000)
    fact_order_items.to_sql("fact_order_items", engine, if_exists="replace", index=False, chunksize=5000)

    with engine.connect() as conn:
        conn.execute(text("""
            ALTER TABLE dim_customer ADD PRIMARY KEY (customer_id);
            ALTER TABLE dim_product  ADD PRIMARY KEY (product_id);
            ALTER TABLE dim_seller   ADD PRIMARY KEY (seller_id);
            ALTER TABLE dim_date     ADD PRIMARY KEY (date_id);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON fact_orders(customer_id);
            CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON fact_orders(date_id);
            CREATE INDEX IF NOT EXISTS idx_fact_items_order ON fact_order_items(order_id);
            CREATE INDEX IF NOT EXISTS idx_fact_items_product ON fact_order_items(product_id);
            CREATE INDEX IF NOT EXISTS idx_fact_items_seller ON fact_order_items(seller_id);
        """))
        conn.commit()

    print(f"[LOAD] Terminé en {round(time.time()-start, 2)}s")

    with engine.connect() as conn:
        for table in ["dim_date","dim_customer","dim_product",
                      "dim_seller","fact_orders","fact_order_items"]:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table:25s} : {count:,} lignes")

# ============================================================
# DÉFINITION DU DAG
# ============================================================
with DAG(
    dag_id          = "pipeline_etl_olist",
    default_args    = default_args,
    description     = "Pipeline ETL complet — Dataset Olist",
    schedule_interval = "0 6 * * *",
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    tags            = ["etl", "olist", "master_mlaim"]
) as dag:

    extract = PythonOperator(
        task_id         = "extract_sources",
        python_callable = task_extract,
    )

    transform = PythonOperator(
        task_id         = "transform_data",
        python_callable = task_transform,
    )

    load = PythonOperator(
        task_id         = "load_postgresql",
        python_callable = task_load,
    )

    # Ordre d'exécution
    extract >> transform >> load