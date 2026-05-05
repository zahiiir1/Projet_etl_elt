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