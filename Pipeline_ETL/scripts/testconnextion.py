from sqlalchemy import create_engine, text

engine = create_engine("postgresql://postgres:F09BB8AD67D5@localhost:5432/ETL")

with engine.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    print("Connexion OK :", result.fetchone()[0])