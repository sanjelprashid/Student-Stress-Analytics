# Student Stress Monitoring — ETL + Dashboard (PySpark, PostgreSQL, Superset)

This project shows a complete **ETL pipeline** using **Python + PySpark** to clean data and load it into **PostgreSQL**. Then we build simple charts and a dashboard in **Apache Superset**.

---

## 1) What you will achieve

- Cleaned data tables in PostgreSQL:
  - `curated_student`
  - `curated_stress_levels`
  - `master_table` 
- A Superset dashboard called **“Student Stress Monitoring”** with:
  - Stress level distribution
  - Student age distribution
  - Responses by gender

---

## 2) Quick Start

1. Install: **Python 3.12**, **Java 8+**, **PostgreSQL**, **Apache Superset**, **Git**.  
2. Download this project from GitHub and unzip.  
3. Open the folder in **VS Code**.
4. In a terminal, run:
   - python -m venv venv
   - source venv/bin/activate         #Windows: venv\Scripts\activate
   - pip install -r requirements.txt

6. Put the two CSV files into `data/raw/` with these names:
   - `Stress_Dataset.csv`
   - `StressLevelDataset.csv`

7. Start PostgreSQL. Note your password for the `postgres` user.  
8. Run the ETL:
   - python etl/transform/execute.py
     
9. Open **Superset** (usually http://localhost:8088), add the Postgres connection, create datasets from the three tables, and open the **Student Stress Monitoring** dashboard.

---

## 3) Requirements (install once)

- **Windows / Mac / Linux**
- **Python 3.12** → download from python.org
- **Java 8+ (OpenJDK)** → required by PySpark
- **PostgreSQL 14+** → download from postgresql.org
- **Apache Superset** → `pip install apache-superset`
- **Git** → download from git-scm.com

---

## 4) Project structure (what’s inside)

```
stress_etl_project/
|- app/
|- config/
|- data/
|  |-raw/                # place your 2 CSVs here
|  |-extracted/
|- etl/
|  |-extract/
|  |-transform/
|  |  |- execute.py        # run this file
|  |-load/
|- logs/
|- README.md
|- requirements.txt
|- run.sh

```

---

## 5) Setup: create a Python environment

Open a terminal **inside the project folder**, then:

```
- python -m venv venv
# Activate it
# LInux:
-source venv/bin/activate
# Windows:
- venv\Scripts\activate
# Install all needed packages
- pip install -r requirements.txt

```

---

## 6) Add the data files

Download or copy these two CSVs and put them into `data/raw/`:

- `Stress_Dataset.csv`
- `StressLevelDataset.csv`

 \\ Keep the exact names. The script looks for these filenames.

---

## 7) Start PostgreSQL (local database)

- Make sure PostgreSQL is running.
- Default local settings (you can keep these):
  - **Host**: `localhost`
  - **Port**: `5432`
  - **Database**: `postgres`
  - **User**: `postgres`
  - **Password**: *(the one you set when installing)*

---

## 8) Run the ETL

From the project folder, with the virtualenv **activated**:

```
- python etl/transform/execute.py

```

What it does:
1. **Extract**: Reads the 2 CSVs from `data/raw/`.
2. **Transform** (PySpark): cleans columns, casts types, removes duplicates, builds a `stress_bucket` (low/moderate/high).
3. **Load**: writes 3 tables to PostgreSQL via JDBC:
   - `curated_student`
   - `curated_stress_levels`
   - `master_table`

### Verify in Postgres 
Open a terminal and run:

```

# Linux/Mac (psql)
- psql -U postgres -h localhost -d postgres
# then inside psql:
- \dt
- SELECT COUNT(*) FROM curated_student;
- SELECT COUNT(*) FROM curated_stress_levels;
- SELECT COUNT(*) FROM master_table;

```

---

## 9) Open Apache Superset and connect to Postgres

1. If it’s your first time:
   ```
   # (inside your Python venv)
   - superset fab create-admin
   - superset db upgrade
   - superset init
   - superset run -p 8088

   ```
   Open http://localhost:8088 and log in.

2. Add the database:
   - Go to **Settings → Database Connections → + DATABASE**
   - then **Connect**.

     \\ After connecting, PostgreSQL appears in **Datasets → Databases**.

---

## 10) Create datasets for the 3 tables

In **Datasets → + DATASET**:
- Select your Postgres connection
- **Schema**: `public`
- **Table**: choose each of:
  - `curated_stress_levels`
  - `curated_student`
  - `master_table`
- Click **Create dataset** for each one.

---

## 11) Build the dashboard 

1) **Stress level distribution**  
   - Select Dataset 
   - Select Chart type
   - Save 

2) **Student age distribution**  
   - Select Dataset 
   - Select Chart type  
   - Save

3) **Responses by Gender**  
   - Select Dataset  
   - Select Chart type  
   - Save 

Now create a dashboard:
- **Dashboards → + DASHBOARD → “Student Stress Monitoring”**
- **Add charts** → pick the three charts above → arrange → **Save**.

---

## 12) How to run again 

Each time you want to re-run with the same setup:
```
# in project folder
- source venv/bin/activate        # Windows: venv\Scripts\activate
- python etl/transform/execute.py
- superset run -p 8088            # if Superset isn’t already running

```

---

## 13) Credits

- Built with **Python, PySpark, PostgreSQL, Apache Superset**.
- Author: Prashiddha Sanjel 
