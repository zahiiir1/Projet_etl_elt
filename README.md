# Projet ETL vs ELT avec Olist Dataset

## Description
Ce projet compare deux approches de traitement de données : ETL et ELT, à partir du dataset Olist.

## Objectifs
- Construire un pipeline ETL avec Python, Pandas et PostgreSQL
- Construire un pipeline ELT avec PostgreSQL et dbt
- Comparer les deux approches selon la performance, la maintenabilité et la simplicité
- Visualiser les résultats avec Power BI

## Stack technique

### ETL
- Python
- Pandas
- PostgreSQL

### ELT
- PostgreSQL
- dbt

### BI
- Power BI

## Structure du projet
```text
etl/        scripts ETL Python
elt/        scripts ELT et dbt
dataset/    données locales non versionnées
docs/       documentation
powerbi/    fichiers ou captures Power BI
dataset/
dataset.zip
logs/
__pycache__/
*.pyc
.env
.venv/
venv/
.ipynb_checkpoints/
# Datasets
dataset/
dataset.zip
*.zip
*.csv
*.xlsx
*.json
*.parquet

# Logs
logs/
*.log

# Python
__pycache__/
*.pyc
.venv/
venv/
env/

# Jupyter
.ipynb_checkpoints/

# Environment variables
.env

# OS
.DS_Store
Thumbs.db