# 🏢 Databricks SQL Warehouse — Practical Implementation & Concepts

![Databricks](https://img.shields.io/badge/Databricks-SQL%20Warehouse-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Editor%20%26%20Queries-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)
![Dimensional Modeling](https://img.shields.io/badge/Dimensional-Modeling-blue?style=for-the-badge)
![SCD](https://img.shields.io/badge/Slowly%20Changing-Dimensions-brightgreen?style=for-the-badge)
![Delta Live Tables](https://img.shields.io/badge/Delta-Live%20Tables-00ADD8?style=for-the-badge)
![Streaming](https://img.shields.io/badge/Streaming-Tables-orange?style=for-the-badge)
![Orchestration](https://img.shields.io/badge/ETL-Jobs%20%26%20Alerts-success?style=for-the-badge)

---

## 📌 Section Overview

This repository documents **hands-on, practical work** with the **Databricks SQL Warehouse** — covering everything from warehouse fundamentals and sizing/scaling, through the SQL editor and query tooling, into dimensional data modeling, streaming and incremental loads, Slowly Changing Dimensions, Auto CDC, and Delta Live Tables, and finishing with production-grade operational concerns: query profiling/caching, scheduling, alerting, and ETL orchestration.

> Understanding what a SQL Warehouse is on paper is one thing. Actually sizing a warehouse for a workload, writing parameterized queries with reusable snippets, wiring up incremental loads with SCD logic, and scheduling alerts on top of it — that's the gap this repo closes.

---

## 🎯 Aim & Objectives

- Understand what a **Databricks SQL Warehouse** is and how it differs from a general-purpose compute cluster
- Master the different **SQL Warehouse types** (Serverless, Pro, Classic) and know when each applies
- Learn **sizing and scaling** — cluster size selection, auto-scaling, and multi-cluster load balancing for concurrency
- Get hands-on with the **Databricks SQL Editor** — writing, organizing, and running queries efficiently
- Use **query parameters** to build dynamic, reusable queries, and **query snippets** to speed up repetitive SQL patterns
- Understand **dimensional data modeling** as applied inside Databricks SQL
- Work with **streaming tables** for continuously updating data
- Implement **incremental data loading** so pipelines only process new or changed data
- Apply **Slowly Changing Dimensions (SCD)** logic to track how dimension data evolves over time
- Use **Auto CDC** (Change Data Capture) in Databricks to simplify upsert/merge logic
- Understand **Delta Live Tables (DLT)** as a declarative framework for building reliable pipelines
- Learn **query profiling and query caching** to diagnose and improve query performance
- Set up **query scheduling** and **alerts** for automated, monitored reporting
- Use **Databricks ETL Jobs** to orchestrate multi-step pipelines end to end
- Explore the **AI Chatbot in Databricks SQL Warehouse** as a productivity aid for querying and exploration

---

## 🧰 Tech Stack & Concepts

| Concept | Purpose |
|---|---|
| Databricks SQL Warehouse | Managed, SQL-optimized compute for running analytical queries at scale |
| Warehouse Types (Serverless / Pro / Classic) | Choosing the right compute model for cost, speed, and workload needs |
| Sizing & Scaling | Matching warehouse size and cluster scaling policy to concurrency and query load |
| SQL Editor | Primary interface for writing, running, and organizing SQL work in Databricks |
| Query Parameters | Making queries dynamic and reusable across different inputs |
| Query Snippets | Reusable blocks of SQL for common patterns, reducing repetitive query writing |
| Dimensional Data Modeling | Structuring data into facts and dimensions for fast, intuitive analytical queries |
| Streaming Tables | Tables that continuously ingest and reflect new data as it arrives |
| Incremental Data Load | Loading only new/changed records instead of reprocessing full datasets |
| Slowly Changing Dimensions (SCD) | Tracking how dimension attributes change over time |
| Auto CDC | Databricks-native change data capture to simplify merge/upsert pipelines |
| Delta Live Tables (DLT) | Declarative pipeline framework with built-in data quality and orchestration |
| Query Profiling & Caching | Diagnosing slow queries and speeding up repeated query execution |
| Query Scheduling & Alerts | Automating recurring reports and getting notified on data conditions |
| ETL Jobs (Orchestration) | Chaining multiple tasks/notebooks into a single managed pipeline |
| AI Chatbot in SQL Warehouse | Using Databricks' built-in assistant to accelerate query writing and exploration |

---

## 🏗️ Learning Architecture

```
Foundations
   └── What is a Databricks SQL Warehouse
   └── Warehouse Types (Serverless / Pro / Classic)
   └── Sizing & Scaling for concurrency
        ↓
Working With the Warehouse
   └── Databricks Overview & SQL Editor
   └── Query Parameters & Query Snippets
        ↓
Modeling & Ingesting Data
   └── Dimensional Data Model Overview
   └── Streaming Tables
   └── Incremental Data Load
        ↓
Handling Change Over Time
   └── Slowly Changing Dimensions
   └── Auto CDC in Databricks
   └── Delta Live Tables
        ↓
Operating in Production
   └── Query Profiling & Query Caching
   └── Query Scheduling & Alerts
   └── ETL Jobs for Orchestration
   └── AI Chatbot in Databricks SQL Warehouse
```

---

## 📖 Detailed Learnings

### 🏛️ 1. Databricks SQL Warehouse Fundamentals
**Focus:** Building a working mental model of what a SQL Warehouse actually is and how it fits into the Databricks platform.

- Understood **what a Databricks SQL Warehouse is** — a managed compute resource purpose-built for running SQL queries against the lakehouse, distinct from general-purpose all-purpose clusters
- Compared the **SQL Warehouse types**: Serverless (instant start, fully managed), Pro (advanced performance features), and Classic (customer-managed cloud infrastructure) — and the trade-offs each makes between cost, startup latency, and control
- Learned **sizing and scaling**: choosing an appropriate warehouse size (X-Small through beyond), configuring auto-stop to control cost, and using multi-cluster auto-scaling to handle concurrent query load without queuing

**Takeaway:** Picking the right warehouse type and size is a cost-vs-performance decision, not a default setting — this section builds the judgment to make that call.

---

### 🖥️ 2. Databricks Overview & SQL Editor
**Focus:** Getting hands-on with the actual working environment used day to day.

- Navigated the **Databricks workspace** end to end — catalogs, schemas, tables, and where SQL work lives relative to notebooks and jobs
- Practiced writing and running queries in the **Databricks SQL Editor**, including organizing queries, viewing results, and working across multiple tabs/queries efficiently

**Takeaway:** Comfort in the SQL Editor is the baseline skill everything else in this repo builds on.

---

### 🧩 3. Query Parameters & Query Snippets
**Focus:** Making SQL work reusable instead of rewriting the same logic over and over.

- Built **parameterized queries**, letting a single query be re-run against different filter values, date ranges, or IDs without editing the SQL itself
- Went beyond hardcoded filters to a fully dynamic query using `IDENTIFIER()`, so even the **catalog, schema, and table name** are passed in as parameters — not just the `WHERE` clause values
- Created **query snippets** for commonly repeated SQL patterns, cutting down repetitive typing and standardizing how common logic (like date filters or joins) gets written across queries

```sql
-- Static, hardcoded filter query
SELECT *
FROM products
WHERE product_id = :product_id AND price > :price

-- Fully dynamic query — catalog, schema, table AND column name are all parameters
SELECT :col1
FROM IDENTIFIER(:catalog || '.' || :schema || '.' || :table)
WHERE IDENTIFIER(:col_product_id) = :product_id AND price > :price
```

```sql
-- Query Snippet — reusable block saved via SQL Editor → ⋮ → View → Query Snippets → Create Snippet
SELECT * FROM products
WHERE product_id = 1
```

**Takeaway:** Parameters and snippets turn one-off queries into reusable, shareable building blocks — using `IDENTIFIER()` to parameterize the catalog/schema/table itself (not just filter values) is what makes a single query reusable across completely different tables.

---

### ⭐ 4. Dimensional Data Model Overview
**Focus:** Applying dimensional modeling concepts directly inside Databricks SQL.

- Reinforced the **fact vs. dimension** split and how a dimensional model supports fast, business-friendly analytical queries
- Connected the dimensional modeling theory to how tables are actually organized and queried inside a Databricks SQL Warehouse

**Takeaway:** Dimensional modeling isn't just a whiteboard exercise — it directly shapes how tables are structured and queried in this environment.

---

### 🌊 5. Streaming Tables & Incremental Data Load
**Focus:** Moving from static, full-refresh tables to tables that stay up to date automatically and efficiently.

- Learned how **streaming tables** continuously ingest new data as it lands, rather than requiring manual, scheduled reloads
- Built streaming tables directly on top of source dimension/fact tables using `CREATE OR REFRESH STREAMING TABLE ... AS SELECT * FROM STREAM(...)`, so each streaming table incrementally tracks new rows appended to its underlying source table
- Implemented **incremental data loading**, so pipelines only process new or changed records instead of reprocessing entire datasets on every run — the same watermark-driven CDC principle applied at the warehouse level

```sql
-- Streaming tables built on top of source dimension/fact tables
CREATE OR REFRESH STREAMING TABLE workspace.stg.stream_passengers
AS SELECT * FROM STREAM(workspace.stg.dim_passengers);

CREATE OR REFRESH STREAMING TABLE workspace.stg.stream_airports
AS SELECT * FROM STREAM(workspace.stg.dim_airports);

CREATE OR REFRESH STREAMING TABLE workspace.stg.stream_bookings
AS SELECT * FROM STREAM(workspace.stg.fact_bookings);
```

**Takeaway:** Streaming tables and incremental loads are what make a pipeline scalable — reprocessing everything on every run simply doesn't work past a certain data volume. `STREAM(...)` on a source table means only newly appended rows flow into the streaming table on each refresh.

---

### 🔁 6. Slowly Changing Dimensions & Auto CDC
**Focus:** Handling dimension data that changes over time, and doing it with Databricks' built-in tooling rather than hand-rolled `MERGE` logic.

- Applied **Slowly Changing Dimension (SCD)** logic to track how dimension attributes evolve — deciding when a change should simply overwrite a value versus when history needs to be preserved
- Learned **Auto CDC** in Databricks — a native change-data-capture mechanism that simplifies the upsert/merge pattern typically needed to keep tables in sync with changing source data, reducing the amount of manual `MERGE` logic required
- Implemented **Auto CDC as SCD Type 2** for two separate dimensions (`DimAirports`, `DimPassengers`) — each fed by its own streaming table, keyed on its natural key, and sequenced so late-arriving/out-of-order records are still applied correctly

```sql
-- Auto CDC into a Type 2 dimension, sourced from a streaming table

CREATE OR REFRESH STREAMING TABLE workspace.enr.DimAirports;

CREATE FLOW flow1
AS AUTO CDC INTO
  DimAirports
FROM stream(stg.stream_airports)
  KEYS (airport_id)
  SEQUENCE BY airport_id
  STORED AS SCD TYPE 2;

CREATE OR REFRESH STREAMING TABLE workspace.enr.DimPassengers;

CREATE FLOW flow2
AS AUTO CDC INTO
  DimPassengers
FROM stream(stg.stream_passengers)
  KEYS (passenger_id)
  SEQUENCE BY passenger_id
  STORED AS SCD TYPE 2;
```

**Takeaway:** `AUTO CDC INTO ... STORED AS SCD TYPE 2` replaces the entire hand-written two-`MERGE` expire-and-insert pattern with a single declarative flow — Databricks handles the row expiry, `is_current` tracking, and new-row insert automatically, keyed and sequenced by whatever natural key you define.

---

### ⚡ 7. Delta Live Tables (DLT)
**Focus:** Understanding the declarative pipeline framework built on top of Delta Lake.

- Learned how **Delta Live Tables** let a pipeline be defined declaratively (what the tables should look like) rather than imperatively (step-by-step ETL code), with Databricks managing execution, dependency ordering, and data quality checks
- Connected DLT back to the **Medallion Architecture** pattern — Bronze/Silver/Gold pipelines can be expressed as DLT pipelines with built-in monitoring

**Takeaway:** DLT is the production-grade evolution of hand-written Bronze → Silver → Gold notebooks — same layers, managed orchestration and quality enforcement.

---

### 📊 8. Query Profiling & Query Caching
**Focus:** Diagnosing and improving query performance rather than just writing queries that work.

- Used **query profiling** to break down where time is spent in a query execution — identifying expensive joins, scans, or shuffles
- Learned how **query caching** in Databricks SQL speeds up repeated execution of the same or similar queries, and when caching helps versus when it doesn't

**Takeaway:** A working query and a fast query aren't the same thing — profiling closes that gap.

---

### ⏰ 9. Query Scheduling, Alerts & ETL Job Orchestration
**Focus:** Turning one-off queries and pipelines into automated, monitored systems.

- Set up **query scheduling** to run reports automatically on a recurring cadence, without manual intervention
- Configured **alerts** to trigger notifications based on query result conditions — catching data issues or business events as they happen rather than after the fact
- Used **Databricks ETL Jobs** to orchestrate multi-step pipelines, chaining notebooks and tasks together with dependencies, retries, and scheduling
- Built an **ETL Job** that chains two tasks in sequence: `Query1` (a filtered lookup against the `products` table) followed by `sqlFile.sql` (which materializes a filtered result into a new table) — demonstrating a multi-task, dependency-ordered job rather than a single standalone query
- Built a separate **Delta Live Tables pipeline** (defined in `SCDs.sql`) that owns the streaming-table-to-SCD-Type-2 flow for `DimAirports` and `DimPassengers` end to end, so the CDC logic runs as a managed, monitored pipeline instead of an ad-hoc notebook

```sql
-- Task 1: Query1 — filtered lookup feeding the job
SELECT
  product_name
FROM
  workspace.warehouse.products
WHERE
  product_name = 'Product A'

-- Task 2: sqlFile.sql — materializes a parameterized filter into a new table
CREATE TABLE workspace.stg.test_table
AS
SELECT * FROM workspace.warehouse.products
WHERE
  product_name = :prod_para
```

```sql
-- Pipeline definition (SCDs.sql) — the streaming + Auto CDC flow, run as a DLT pipeline
CREATE OR REFRESH STREAMING TABLE workspace.enr.DimAirports;

CREATE FLOW flow1
AS AUTO CDC INTO
  DimAirports
FROM stream(stg.stream_airports)
  KEYS (airport_id)
  SEQUENCE BY airport_id
  STORED AS SCD TYPE 2;

CREATE OR REFRESH STREAMING TABLE workspace.enr.DimPassengers;

CREATE FLOW flow2
AS AUTO CDC INTO
  DimPassengers
FROM stream(stg.stream_passengers)
  KEYS (passenger_id)
  SEQUENCE BY passenger_id
  STORED AS SCD TYPE 2;
```

**Takeaway:** This is where a pipeline stops being something you run manually and becomes something that runs — and watches — itself. The **Job** (`Query1` → `sqlFile.sql`) shows task-level orchestration with dependencies; the **Pipeline** (`SCDs.sql`) shows the same idea applied to a continuous, declarative DLT flow.

#### 📸 Job & Pipeline Runs

<!--
Add screenshots of the completed Job run and the Delta Live Tables Pipeline graph here, e.g.:

![ETL Job Run](./screenshots/etl-job-run.png)
![DLT Pipeline Graph](./screenshots/dlt-pipeline-graph.png)
-->


---

### 🤖 10. AI Chatbot in Databricks SQL Warehouse
**Focus:** Using Databricks' built-in AI assistant as a practical accelerant for SQL work.

- Explored the **AI Chatbot** available inside the Databricks SQL Warehouse interface for query generation, explanation, and troubleshooting
- Used it as a productivity tool alongside manual query writing — speeding up exploration of unfamiliar tables and schemas

**Takeaway:** Even with strong SQL fundamentals, the built-in assistant is a genuine time-saver for exploration and first-draft queries.

---

## 🧠 Skills Demonstrated — Full Mastery Checklist

| Databricks SQL Warehouse Domain | Demonstrated Through |
|---|---|
| ✅ Warehouse Fundamentals | What a SQL Warehouse is, warehouse types, sizing & scaling |
| ✅ Core Tooling | Databricks Overview, SQL Editor, Query Parameters, Query Snippets |
| ✅ Dimensional Modeling | Dimensional Data Model Overview applied in a Databricks SQL context |
| ✅ Ingestion Patterns | Streaming Tables, Incremental Data Load |
| ✅ Change Management | Slowly Changing Dimensions, Auto CDC |
| ✅ Declarative Pipelines | Delta Live Tables overview and Medallion Architecture connection |
| ✅ Performance Tuning | Query Profiling, Query Caching |
| ✅ Production Operations | Query Scheduling, Alerts, ETL Job Orchestration |
| ✅ AI-Assisted Workflow | AI Chatbot in Databricks SQL Warehouse |

> **In short:** this repo isn't just "I watched a Databricks SQL Warehouse course" — it's a working record of setting up warehouses, writing dynamic and reusable SQL, applying dimensional modeling and SCD logic, wiring up incremental/streaming ingestion, and operating the whole thing with profiling, caching, scheduling, alerting, and job orchestration. **This reflects practical, end-to-end fluency with the Databricks SQL Warehouse — from setup to production operation.**

---

## ▶️ How to Use This Repo

### Prerequisites
- A Databricks Workspace with access to a SQL Warehouse (Serverless, Pro, or Classic)
- Familiarity with basic SQL

### Suggested Path
1. Start with **Warehouse Fundamentals** — understand warehouse types and sizing before writing any queries
2. Move to the **SQL Editor, Query Parameters, and Snippets** to get comfortable with the day-to-day workflow
3. Study **Dimensional Data Modeling**, then apply it through **Streaming Tables** and **Incremental Data Load**
4. Implement **Slowly Changing Dimensions** and explore **Auto CDC** as the managed alternative
5. Review **Delta Live Tables** to see the declarative pipeline approach
6. Finish with the operational layer: **Query Profiling & Caching**, **Scheduling & Alerts**, and **ETL Job Orchestration**

---

