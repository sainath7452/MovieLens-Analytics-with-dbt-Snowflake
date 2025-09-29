# MovieLens Analytics with dbt & Snowflake  

## 📌 Project Overview  
This project demonstrates how to build a modern data pipeline using **dbt (Data Build Tool)** and **Snowflake** with the famous **MovieLens dataset**.  

The goal is to transform raw CSVs into analytics-ready tables, apply **Slowly Changing Dimensions (SCD2)**, and generate insights such as:  
- How user ratings evolve over time  
- How tags change historically (SCD2)  
- How to create tag-based recommendation models  

---

## 🎯 Business Problem  
Entertainment platforms (like Netflix, Prime, etc.) need to understand:  
- **User preferences** → Which movies and genres are trending?  
- **Rating patterns** → How do ratings change across time, genres, or users?  
- **Tag evolution** → How do user-generated tags evolve, and can they be used for recommendations?  

This project answers these questions by:  
✅ Standardizing raw CSVs into clean staging tables  
✅ Building fact & dimension models in dbt  
✅ Tracking slowly changing dimensions (SCD2) with dbt snapshots  
✅ Producing analytics marts for BI dashboards  

---

## 📊 Raw Data & Sources  

1. **ratings.csv**  
   Columns: `user_id, movie_id, rating, timestamp`  
   Example: `1, 1193, 5.0, 978300760`  

2. **movies.csv**  
   Columns: `movie_id, title, genres`  
   Example: `1, Toy Story (1995), Adventure`  

3. **tags.csv**  
   Columns: `user_id, movie_id, tag, tagged_at`  
   Example: `15, 318, cult classic, 2025-01-10`  

4. **genome-scores.csv**  
   Columns: `movie_id, tag_id, relevance`  
   Example: `1, 2, 0.80`  

5. **genome-tags.csv**  
   Columns: `tag_id, tag`  
   Example: `1, action`  

6. **Seeds (custom CSVs)**  
   - `movie_decades.csv` → Mapping of `year → decade`  
   - `ratings_labels.csv` → Maps rating scores to categories (e.g., Excellent, Average, Poor)  
   - `tag_category.csv` → Groups tags into categories  

---

## 📂 Project Structure  

```bash
movielens_dbt/
│── models/
│   ├── staging/       # Raw → Staging transformations
│   ├── dim/           # Dimension models (movies, tags, genome_tags)
│   ├── marts/         # Fact & aggregated models (ratings, tags, performance)
│
│── snapshots/         # dbt Snapshots (SCD2 for tags)
│── seeds/             # Seed CSVs (e.g., decades lookup, ratings labels, tag categories)
│── macros/            # Custom macros (dbt_utils, surrogate keys)
│── tests/             # Custom & generic dbt tests
│── analyses/          # Ad-hoc analysis queries
│
│── dbt_project.yml    # Project configuration
│── packages.yml       # dbt packages (dbt-utils, etc.)
│── README.md          # Project documentation

⚙️ Transformations & Models
1. Staging Layer (models/staging/)

Cleaned & standardized raw CSVs

Converted timestamps, split genres, ensured primary keys

2. Dimension Layer (models/dim/)

dim_movies → Movie attributes (title, genres, year, decade from seed)

dim_users → User-level attributes

dim_tags → Tags + categories (with SCD2 tracking using snapshots)

dim_genome_tags → Standardized genome tags

3. Fact Layer (models/marts/)

fact_ratings → Ratings fact table (linked to movies, users, and rating labels)

fact_tags → Tagging activity fact table

fact_movie_performance → Aggregated performance metrics by decade, genre, etc.

4. Snapshots (snapshots/)

snap_tags → SCD2 implementation for tags (user_id + movie_id + tag as unique key)

🔍 Key Features

SCD2 with dbt Snapshots → Historical tag evolution

Seeds → Mapping tables for decades, ratings, and categories

Surrogate Keys → Consistent joins across fact & dim tables

Analytics Marts → Ready for BI dashboards (Power BI, Looker, etc.)

📈 Example Analytics Questions Answered

What are the most popular genres by decade?

How do user ratings change over time?

Which tags are trending historically (SCD2)?

Can we build recommendations based on tags and genome relevance?

🚀 How to Run

Install dbt dependencies:

dbt deps


Run seeds (load lookup tables):

dbt seed


Run models:

dbt run


Run snapshots:

dbt snapshot


Test everything:

dbt test

📌 Tech Stack

Snowflake → Cloud Data Warehouse

dbt Core → Data transformations, SCD2, testing

dbt-utils → Macros & helper functions

Power BI / Looker Studio → Downstream BI visualization

✨ Future Improvements

Add recommendation engine models (collaborative filtering)

Enhance genre hierarchy analysis (nested genres)

Build real-time pipeline using dbt Cloud + Airflow
