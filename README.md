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

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-150458?logo=pandas&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-4169E1?logo=postgresql&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?logo=apacheairflow&logoColor=white)

### ELT

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-Analytical%20Database-FFF000?logo=duckdb&logoColor=black)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?logo=dbt&logoColor=white)

### BI

![Power BI](https://img.shields.io/badge/Power%20BI-Visualization-F2C811?logo=powerbi&logoColor=black)
## Dépendances principales

![pandas](https://img.shields.io/badge/pandas-DataFrame-150458?logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-ORM%20%26%20SQL-D71F00)
![python-dotenv](https://img.shields.io/badge/python--dotenv-Environment%20Variables-green)
![duckdb](https://img.shields.io/badge/duckdb-Embedded%20OLAP-FFF000?logo=duckdb&logoColor=black)
![dbt-duckdb](https://img.shields.io/badge/dbt--duckdb-dbt%20Adapter-FF694B?logo=dbt&logoColor=white)
<<<<<<< HEAD
=======
````markdown
## Installation

Cloner le projet :

```bash
git clone https://github.com/zahiiir1/Projet_etl_elt.git
cd Projet_etl_elt
pip install -r requirements.txt
>>>>>>> ad5b008 (Add installation instructions and requirements)

## Structure du projet

```text
Projet_etl_elt/
│
├── Pipeline_ETL/                         # Pipeline ETL avec Python, Pandas, PostgreSQL et Airflow
│   ├── dags/                             # DAG Airflow pour orchestrer le pipeline ETL
│   ├── dataset/                          # Données CSV locales non versionnées
│   ├── logs/                             # Logs locaux non versionnés
│   └── scripts/                          # Scripts Python du pipeline ETL
│
├── Pipeline_ELT/                         # Pipeline ELT avec Python, DuckDB et dbt
│   ├── dataset/                          # Données CSV locales non versionnées
│   ├── exports/                          # Exports éventuels des tables finales
│   ├── logs/                             # Logs locaux non versionnés
│   ├── olist_dbt/                        # Projet dbt pour les transformations SQL
│   │   ├── models/
│   │   │   ├── staging/                  # Vues dbt de nettoyage et standardisation
│   │   │   └── marts/                    # Tables finales dim/fact pour Power BI
│   │   ├── dbt_project.yml               # Configuration dbt
│   │   └── README.md                     # Documentation dbt
│   ├── scripts/                          # Scripts Python du pipeline ELT
│   └── olist_raw.duckdb                  # Base DuckDB locale non versionnée
│
├── powerbi/                              # Fichiers ou captures Power BI
├── docs/                                 # Documentation du projet
├── .env.example                          # Exemple de configuration sans secret
├── .gitignore                            # Fichiers ignorés par Git
└── README.md                             # Présentation du projet
