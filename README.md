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
├── dataset/              # Données locales non versionnées
├── scripts/              # Scripts Python pour le pipeline ETL
├── docs/                 # Documentation du projet
├── powerbi/              # Fichiers ou captures Power BI
├── logs/                 # Logs locaux non versionnés
├── .gitignore            # Fichiers ignorés par Git
└── README.md             # Présentation du projet