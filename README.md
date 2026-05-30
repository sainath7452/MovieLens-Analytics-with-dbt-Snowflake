# 🎬 MovieLens Analytics — dbt + Snowflake

Modern analytics engineering project applying dbt best practices to the MovieLens dataset — staging, mart layers, data quality tests, and full documentation in Snowflake.

---

## 📌 Project Summary

| | |
|---|---|
| **Dataset** | MovieLens — movies, ratings, users, tags |
| **Warehouse** | Snowflake |
| **Transformation** | dbt Core |
| **Modelling Pattern** | Staging → Intermediate → Mart |
| **Data Quality** | dbt tests on all models |

---

## 🏗 Architecture

```
Raw Source Data (MovieLens CSVs)
        │
        ▼
  Snowflake Raw Schema
        │
        ▼
  dbt Staging Layer    ──► Cleaned, typed, renamed sources
        │
        ▼
  dbt Mart Layer       ──► fact_ratings, dim_movies, dim_users
        │
        ▼
  Analytics / BI Layer
```

---

## 📐 dbt Project Structure

```
models/
  staging/
    ├── stg_movies.sql
    ├── stg_ratings.sql
    ├── stg_users.sql
    └── stg_tags.sql
  marts/
    ├── fact_ratings.sql
    ├── dim_movies.sql
    └── dim_users.sql
```

---

## ✅ Data Quality

All models include dbt tests: `not_null`, `unique`, `accepted_values`, and custom range tests on ratings (0.5–5.0).

---

## 🚀 How to Run

```bash
git clone https://github.com/sainath7452/MovieLens-Analytics-with-dbt-Snowflake
pip install dbt-snowflake
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

---

## 📫 Author

**Sainath Panga** — BI Developer & Analytics Engineer
[LinkedIn](https://linkedin.com/in/sainathreddy-panga) · [GitHub](https://github.com/sainath7452)
