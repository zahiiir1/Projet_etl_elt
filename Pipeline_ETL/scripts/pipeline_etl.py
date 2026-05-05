#-*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine, text
import time
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os
print("=" * 60)
print("PIPELINE ETL -- DATASET OLIST")
print("=" * 60)

# ============================================================
# CONFIGURATION
# ============================================================
DATA_PATH = r"C:/projet_etl\data"
load_dotenv()
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
DB_URL = f"postgresql://postgres:{quote_plus(DB_PASSWORD)}@localhost:5432/ETL"

engine = create_engine(DB_URL)
start_total = time.time()

# ============================================================
# eTAPE 1 : EXTRACT
# ============================================================
print("\n[1/5] EXTRACT -- Lecture des fichiers CSV...")
start = time.time()

orders      = pd.read_csv(f"C:/Users/asus/Desktop/MLAIM/s2/PBI/Projet_etl_elt/dataset/olist_orders_dataset.csv")
customers   = pd.read_csv(f"C:/Users/asus/Desktop/MLAIM/s2/PBI/Projet_etl_elt/dataset/olist_customers_dataset.csv")
items       = pd.read_csv(f"C:/Users/asus\Desktop/MLAIM/s2/PBI/Projet_etl_elt/dataset/olist_order_items_dataset.csv")
products    = pd.read_csv(f"C:/Users/asus\Desktop/MLAIM/s2/PBI/Projet_etl_elt/dataset/olist_products_dataset.csv")
sellers     = pd.read_csv(f"C:/Users/asus\Desktop/MLAIM/s2/PBI/Projet_etl_elt/dataset/olist_sellers_dataset.csv")
payments    = pd.read_csv(f"C:/Users/asus\Desktop/MLAIM/s2/PBI/Projet_etl_elt/dataset/olist_order_payments_dataset.csv")
reviews     = pd.read_csv(f"C:/Users/asus\Desktop/MLAIM/s2/PBI/Projet_etl_elt/dataset/olist_order_reviews_dataset.csv")
translation = pd.read_csv(f"C:/Users/asus\Desktop/MLAIM/s2/PBI/Projet_etl_elt/dataset/product_category_name_translation.csv")

print(f"    orders     : {len(orders):,} lignes")
print(f"    customers  : {len(customers):,} lignes")
print(f"    items      : {len(items):,} lignes")
print(f"    products   : {len(products):,} lignes")
print(f"    sellers    : {len(sellers):,} lignes")
print(f"    payments   : {len(payments):,} lignes")
print(f"    reviews    : {len(reviews):,} lignes")
print(f"  Duree Extract : {round(time.time()-start, 2)}s")

# ============================================================
# eTAPE 2 : TRANSFORM -- Nettoyage
# ============================================================
print("\n[2/5] TRANSFORM -- Nettoyage...")
start = time.time()

# T1 -- Conversion dates
date_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]
for col in date_cols:
    orders[col] = pd.to_datetime(orders[col], errors="coerce")
items["shipping_limit_date"] = pd.to_datetime(
    items["shipping_limit_date"], errors="coerce"
)
print(" T1  -- Dates converties en datetime")

# T2 -- Traduction categories portugais -> anglais
products = products.merge(translation, on="product_category_name", how="left")
print(" T2  -- Jointure traduction effectuee")

# T3 -- Nulls commentaires -> chaine vide
reviews["review_comment_title"]   = reviews["review_comment_title"].fillna("")
reviews["review_comment_message"] = reviews["review_comment_message"].fillna("")
print(" T3  -- Nulls commentaires remplaces")

# T5 -- Nulls colonnes numeriques produits
products["product_category_name"] = (
    products["product_category_name"].fillna("unknown")
)
products["product_category_name_english"] = (
    products["product_category_name_english"].fillna("unknown")
)
for col in ["product_weight_g", "product_length_cm",
            "product_height_cm", "product_width_cm",
            "product_photos_qty"]:
    products[col] = products[col].fillna(0)
print(" T5  -- Nulls produits remplaces par 0")

# T6 -- Standardisation texte
customers["customer_city"]  = customers["customer_city"].str.strip().str.lower()
customers["customer_state"] = customers["customer_state"].str.strip().str.upper()
sellers["seller_city"]      = sellers["seller_city"].str.strip().str.lower()
sellers["seller_state"]     = sellers["seller_state"].str.strip().str.upper()
products["product_category_name_english"] = (
    products["product_category_name_english"].str.strip().str.lower()
)
print(" T6  -- Texte standardise")

# T13 -- Arrondi 2 decimales
items["price"]            = items["price"].round(2)
items["freight_value"]    = items["freight_value"].round(2)
payments["payment_value"] = payments["payment_value"].round(2)
print(" T13 -- Montants arrondis a 2 decimales")

# ---- CONTRÔLE QUALITe ----
print("\n  -- Contrôle qualite --")
print(f"    Commandes sans paiement : {orders[~orders['order_id'].isin(payments['order_id'])].shape[0]}")
print(f"    Commandes sans review   : {orders[~orders['order_id'].isin(reviews['order_id'])].shape[0]}")
print(f"    Commandes sans items    : {orders[~orders['order_id'].isin(items['order_id'])].shape[0]}")
print(f"    Produits sans categorie : {products['product_category_name_english'].isna().sum()}")
print(f"    Prix <= 0               : {(items['price'] <= 0).sum()}")
print(f"    Frais livraison < 0     : {(items['freight_value'] < 0).sum()}")

print(f"  Duree Nettoyage : {round(time.time()-start, 2)}s")

# ============================================================
# eTAPE 3 : TRANSFORM -- Enrichissement
# ============================================================
print("\n[3/5] TRANSFORM -- Enrichissement et nouvelles colonnes...")
start = time.time()

# T4 -- Agregation paiements (type = montant le plus eleve)
payments_agg = payments.groupby("order_id").agg(
    montant_total = ("payment_value", "sum"),
    nb_paiements  = ("payment_sequential", "max"),
    nb_versements = ("payment_installments", "max")
).reset_index()
main_payment = (
    payments.sort_values("payment_value", ascending=False)
    .drop_duplicates("order_id")[["order_id", "payment_type"]]
    .rename(columns={"payment_type": "type_paiement"})
)
payments_agg = payments_agg.merge(main_payment, on="order_id", how="left")
payments_agg["montant_total"] = payments_agg["montant_total"].round(2)
print(" T4  -- Paiements agreges")

# T5 -- Delai livraison en jours
orders["delai_livraison_jours"] = (
    orders["order_delivered_customer_date"] -
    orders["order_purchase_timestamp"]
).dt.days
print(" T5  -- Delai livraison calcule")

# T6 -- Avance/retard livraison
orders["livraison_en_avance"] = (
    orders["order_estimated_delivery_date"] -
    orders["order_delivered_customer_date"]
).dt.days
print(" T6  -- Avance/retard calcule")

# T14 a T17 -- Colonnes temporelles
orders["annee_achat"]        = orders["order_purchase_timestamp"].dt.year
orders["mois_achat"]         = orders["order_purchase_timestamp"].dt.month
orders["trimestre_achat"]    = orders["order_purchase_timestamp"].dt.quarter
orders["jour_semaine_achat"] = orders["order_purchase_timestamp"].dt.day_name()
print(" T14-17 -- Colonnes temporelles creees")

# T18 -- Montant total article
items["montant_total_article"] = (
    items["price"] + items["freight_value"]
).round(2)
print(" T18 -- montant_total_article creee")

# T19 -- Part frais livraison %
items["part_frais_livraison"] = (
    items["freight_value"] /
    items["montant_total_article"].replace(0, pd.NA) * 100
).round(2)
print(" T19 -- part_frais_livraison creee")

# T20 -- Tranche prix
def tranche_prix(price):
    if price < 50:    return "cheap"
    elif price < 200: return "medium"
    else:             return "premium"
items["tranche_prix"] = items["price"].apply(tranche_prix)
print(" T20 -- tranche_prix creee")

# T21 -- Statut livraison (CORRIGe)
def statut_livraison(jours):
    if pd.isna(jours): return "non livre"
    elif jours > 0:    return "en avance"
    elif jours == 0:   return "a temps"
    else:              return "en retard"
orders["statut_livraison"] = orders["livraison_en_avance"].apply(statut_livraison)
print(" T21 -- statut_livraison creee (corrigee)")

# T22 -- Livraison rapide (CORRIGe avec notna)
orders["livraison_rapide"] = orders["delai_livraison_jours"].apply(
    lambda x: 1 if pd.notna(x) and x < 7 else 0
).astype("Int64")
print(" T22 -- livraison_rapide creee (corrigee)")

# T23 -- Satisfaction 3 niveaux
def classer_satisfaction(score):
    if pd.isna(score): return "inconnu"
    elif score >= 4:   return "positif"
    elif score == 3:   return "neutre"
    else:              return "negatif"
reviews["satisfaction"] = reviews["review_score"].apply(classer_satisfaction)
print(" T23 -- satisfaction creee (positif/neutre/negatif)")

# T24 -- A commente
reviews["a_commente"] = (
    reviews["review_comment_message"].str.strip() != ""
).astype(int)
print(" T24 -- a_commente creee")

# Agregation reviews par moyenne
reviews_agg = reviews.groupby("order_id").agg(
    review_score = ("review_score", "mean"),
    nb_reviews   = ("review_id", "count"),
    a_commente   = ("a_commente", "max")
).reset_index()
reviews_agg["review_score"] = reviews_agg["review_score"].round(2)
reviews_agg["satisfaction"] = reviews_agg["review_score"].apply(classer_satisfaction)
print(" Reviews agregees par moyenne")

# Volume produit
products["volume_cm3"] = (
    products["product_length_cm"] *
    products["product_height_cm"] *
    products["product_width_cm"]
).round(2)
print(" volume_cm3 calcule")

print(f"  Duree Enrichissement : {round(time.time()-start, 2)}s")

# ============================================================
# eTAPE 4 : TRANSFORM -- Schema en etoile
# ============================================================
print("\n[4/5] TRANSFORM -- Construction schema en etoile...")
start = time.time()

# DIM_CUSTOMER
dim_customer = customers[[
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
]].drop_duplicates(subset=["customer_id"]).rename(columns={
    "customer_zip_code_prefix" : "zip_code_prefix",
    "customer_city"            : "city",
    "customer_state"           : "state"
})

# DIM_PRODUCT
dim_product = products[[
    "product_id",
    "product_category_name_english",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "volume_cm3"
]].drop_duplicates(subset=["product_id"]).rename(columns={
    "product_category_name_english" : "category",
    "product_weight_g"              : "weight_g",
    "product_length_cm"             : "length_cm",
    "product_height_cm"             : "height_cm",
    "product_width_cm"              : "width_cm"
})

# DIM_SELLER
dim_seller = sellers[[
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
]].drop_duplicates(subset=["seller_id"]).rename(columns={
    "seller_zip_code_prefix" : "zip_code_prefix",
    "seller_city"            : "city",
    "seller_state"           : "state"
})

# DIM_DATE
toutes_dates = orders["order_purchase_timestamp"].dropna()
dim_date = pd.DataFrame({"date": toutes_dates.dt.date.unique()})
dim_date["date"]         = pd.to_datetime(dim_date["date"])
dim_date["date_id"]      = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
dim_date["day"]          = dim_date["date"].dt.day
dim_date["month"]        = dim_date["date"].dt.month
dim_date["month_name"]   = dim_date["date"].dt.month_name()
dim_date["quarter"]      = dim_date["date"].dt.quarter
dim_date["year"]         = dim_date["date"].dt.year
dim_date["day_of_week"]  = dim_date["date"].dt.day_name()
dim_date["week_number"]  = dim_date["date"].dt.isocalendar().week.astype(int)
dim_date["is_weekend"]   = dim_date["date"].dt.dayofweek.isin([5,6]).astype(int)
dim_date = dim_date.drop_duplicates(subset=["date_id"])

# FACT_ORDERS
faits = orders.merge(customers[["customer_id"]], on="customer_id", how="left")
faits = faits.merge(payments_agg, on="order_id", how="left")
faits = faits.merge(reviews_agg, on="order_id", how="left")
faits["date_id"] = (
    faits["order_purchase_timestamp"].dt.strftime("%Y%m%d").astype("Int64")
)

fact_orders = faits[[
    "order_id", "customer_id", "date_id", "order_status",
    "montant_total", "nb_versements", "type_paiement", "nb_paiements",
    "delai_livraison_jours", "livraison_en_avance",
    "statut_livraison", "livraison_rapide",
    "annee_achat", "mois_achat", "trimestre_achat", "jour_semaine_achat",
    "review_score", "satisfaction", "a_commente", "nb_reviews"
]].rename(columns={"order_status": "status"})

# FACT_ORDER_ITEMS
fact_order_items = items[[
    "order_id", "order_item_id", "product_id", "seller_id",
    "shipping_limit_date", "price", "freight_value",
    "montant_total_article", "part_frais_livraison", "tranche_prix"
]].rename(columns={
    "montant_total_article"  : "total_amount",
    "part_frais_livraison"   : "freight_pct",
    "tranche_prix"           : "price_range"
})

# Validation finale
assert fact_orders["order_id"].is_unique,     "Doublons dans fact_orders !"
assert dim_customer["customer_id"].is_unique, "Doublons dans dim_customer !"
assert dim_product["product_id"].is_unique,   "Doublons dans dim_product !"
assert dim_seller["seller_id"].is_unique,     "Doublons dans dim_seller !"
assert dim_date["date_id"].is_unique,         "Doublons dans dim_date !"

print(f"  fact_orders      : {len(fact_orders):,} lignes")
print(f"  fact_order_items : {len(fact_order_items):,} lignes")
print(f"  dim_customer     : {len(dim_customer):,} lignes")
print(f"  dim_product      : {len(dim_product):,} lignes")
print(f"  dim_seller       : {len(dim_seller):,} lignes")
print(f"  dim_date         : {len(dim_date):,} lignes")
print(f"  Duree Schema : {round(time.time()-start, 2)}s")

# ============================================================
# eTAPE 5 : LOAD -- Chargement dans PostgreSQL
# ============================================================
print("\n[5/5] LOAD -- Chargement dans PostgreSQL...")
start = time.time()

dim_date.to_sql(
    "dim_date", engine, if_exists="replace", index=False)
print(" dim_date chargee")

dim_customer.to_sql(
    "dim_customer", engine, if_exists="replace", index=False)
print(" dim_customer chargee")

dim_product.to_sql(
    "dim_product", engine, if_exists="replace", index=False)
print(" dim_product chargee")

dim_seller.to_sql(
    "dim_seller", engine, if_exists="replace", index=False)
print(" dim_seller chargee")

fact_orders.to_sql(
    "fact_orders", engine,
    if_exists="replace", index=False, chunksize=5000)
print(" fact_orders chargee")

fact_order_items.to_sql(
    "fact_order_items", engine,
    if_exists="replace", index=False, chunksize=5000)
print(" fact_order_items chargee")

# Cles primaires + index (POINT 1 -- drop_duplicates deja fait)
print("\n  -- Ajout cles primaires et index --")
with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_customer ADD PRIMARY KEY (customer_id);
        ALTER TABLE dim_product  ADD PRIMARY KEY (product_id);
        ALTER TABLE dim_seller   ADD PRIMARY KEY (seller_id);
        ALTER TABLE dim_date     ADD PRIMARY KEY (date_id);
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_fact_orders_customer
            ON fact_orders(customer_id);
        CREATE INDEX IF NOT EXISTS idx_fact_orders_date
            ON fact_orders(date_id);
        CREATE INDEX IF NOT EXISTS idx_fact_items_order
            ON fact_order_items(order_id);
        CREATE INDEX IF NOT EXISTS idx_fact_items_product
            ON fact_order_items(product_id);
        CREATE INDEX IF NOT EXISTS idx_fact_items_seller
            ON fact_order_items(seller_id);
    """))
    conn.commit()
print(" Cles primaires et index crees")

print(f"  Duree Load : {round(time.time()-start, 2)}s")

# ============================================================
# ReSUMe FINAL
# ============================================================
print("\n" + "=" * 60)
print("PIPELINE ETL TERMINe AVEC SUCCÈS")
print(f"Duree totale : {round(time.time()-start_total, 2)}s")
print("=" * 60)

with engine.connect() as conn:
    for table in [
        "dim_date", "dim_customer", "dim_product",
        "dim_seller", "fact_orders", "fact_order_items"
    ]:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {table}")
        ).scalar()
        print(f"  {table:25s} : {count:,} lignes dans PostgreSQL")

print("\nForeign Keys (non appliquees -- voir rapport) :")
print("  ALTER TABLE fact_orders ADD CONSTRAINT fk_customer")
print("    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id);")
print("  ALTER TABLE fact_orders ADD CONSTRAINT fk_date")
print("    FOREIGN KEY (date_id) REFERENCES dim_date(date_id);")
print("  ALTER TABLE fact_order_items ADD CONSTRAINT fk_product")
print("    FOREIGN KEY (product_id) REFERENCES dim_product(product_id);")
print("  ALTER TABLE fact_order_items ADD CONSTRAINT fk_seller")
print("    FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_id);")