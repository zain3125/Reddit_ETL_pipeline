# Reddit Airflow Data Pipeline

## Overview

This project is an **end-to-end data pipeline** built with **Apache Airflow**, **Docker**, and **PostgreSQL**, designed to extract Reddit posts from multiple subreddits, transform them, and load them into a database for further analysis.

---

## 🧩 Project Structure

```
.
├── airflow.env                # Environment variables for Airflow
├── dags                       # Airflow DAGs folder
│   └── reddit_dag.py          # DAG definition for the Reddit pipeline
├── docker-compose.yml         # Docker services configuration
├── Dockerfile                 # Custom Airflow image build file
├── etls                       # ETL logic for Reddit data
│   └── reddit_etl.py          # Extract, transform, and load functions
├── pipelines                  # Orchestrates ETL tasks
│   └── reddit_pipeline.py     # Connects ETL with Airflow DAG
├── requirements.txt           # Python dependencies
└── utils                      # Helper constants and configurations
    └── constants.py
```

---

## ⚙️ Technologies Used

* **Apache Airflow 2.7.1** for orchestration
* **Docker & Docker Compose** for containerization
* **PostgreSQL 12** as the data warehouse
* **Redis** as the Celery backend
* **PRAW (Python Reddit API Wrapper)** for Reddit API access
* **Pandas & NumPy** for data transformation

---

## 🧠 Pipeline Workflow

### 1. Extract

The DAG starts by extracting data from multiple subreddits (e.g., `Egypt`, `CAIRO`, `AlexandriaEgy`, `Masr`) using Reddit API credentials defined in `constants.py`.

### 2. Transform

Extracted posts are cleaned and standardized:

* Convert timestamps to datetime
* Normalize text columns
* Drop unused fields
* Rename and reorder columns

### 3. Load

Transformed data is loaded into a PostgreSQL database table `reddit_posts`. The table is created automatically if it doesn’t exist.

### 4. Schedule

The DAG runs **daily** (`@daily`) and consists of two main tasks:

* `extract_reddit_data`
* `load_data_to_database`

---

## 🚀 How to Run

### 1. Clone the repository

```bash
git clone <repo_url>
cd <project_directory>
```

### 2. Configure Reddit API credentials

Edit `utils/constants.py`:

```python
CLIENT_ID = "your_reddit_client_id"
SECRET = "your_reddit_secret"
USER_AGENT = "your_user_agent"
```

### 3. Build and start Docker containers

```bash
docker-compose up --build
```

### 4. Initialize Airflow

If not automatically done by `airflow-init` service:

```bash
docker-compose run airflow-init
```

### 5. Access the Airflow Web UI

Visit [http://localhost:8080](http://localhost:8080) and log in with:

```
Username: airflow
Password: airflow
```

### 6. Trigger the DAG

In the Airflow UI, enable and manually trigger **`reddit_pipeline_dag`**.

---

## 🗄️ PostgreSQL Database

The Reddit posts are stored in the table:

```sql
CREATE TABLE reddit_posts (
    id TEXT PRIMARY KEY,
    subreddit_name_prefixed TEXT,
    title TEXT,
    flair TEXT,
    selftext TEXT,
    score INT,
    num_comments INT,
    author TEXT,
    created_utc TIMESTAMP,
    url TEXT,
    over_18 BOOLEAN,
    edited BOOLEAN,
    spoiler BOOLEAN,
    stickied BOOLEAN
);
```

Database connection parameters are defined in `utils/constants.py` under `PG_PARAMS`.

---

## 🧾 Logs & Data

* Logs are stored in `./logs`
* Extracted CSV or JSON data can be found in `./data`

---

## 🧰 Common Commands

```bash
# Stop all containers
docker-compose down

# Restart Airflow services
docker-compose restart

# View Airflow logs
docker-compose logs -f airflow-webserver

# Open Airflow scheduler logs
docker-compose logs -f airflow-scheduler
```

---

## 👨‍💻 Author

**Zain** — Data Engineer | Airflow & PostgreSQL Enthusiast
