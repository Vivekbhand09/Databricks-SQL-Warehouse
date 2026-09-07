# Databricks notebook source
# MAGIC %md
# MAGIC # 🧠 Databricks SQL Warehousing — Complete Interview-Ready Guide
# MAGIC
# MAGIC > One file. Every topic you studied today, explained in depth, with syntax, examples, real interview Q&A, and "gotchas" interviewers love to ask.
# MAGIC > Topics covered: SQL Warehouse (concept, types, sizing/scaling/queuing), Query Parameters, Query Snippets, Streaming Tables, SCD1, SCD2, AUTO CDC, Databricks Pipelines (Lakeflow Declarative Pipelines / DLT), Query Profiling, Query Caching, Query Scheduling, SQL Alerts, SQL Query vs SQL File, and ETL Job Orchestration.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📑 Table of Contents
# MAGIC
# MAGIC 1. [What is a SQL Warehouse](#1-what-is-a-sql-warehouse)
# MAGIC 2. [Types of SQL Warehouses](#2-types-of-sql-warehouses)
# MAGIC 3. [Warehouse Sizing, Scaling & Queuing Behavior](#3-warehouse-sizing-scaling--queuing-behavior)
# MAGIC 4. [Query Parameters](#4-query-parameters)
# MAGIC 5. [Query Snippets](#5-query-snippets)
# MAGIC 6. [Streaming Tables](#6-streaming-tables)
# MAGIC 7. [SCD Type 1 (Slowly Changing Dimension)](#7-scd-type-1)
# MAGIC 8. [SCD Type 2 (Slowly Changing Dimension)](#8-scd-type-2)
# MAGIC 9. [AUTO CDC in Databricks](#9-auto-cdc-in-databricks)
# MAGIC 10. [Databricks Pipelines (Lakeflow Declarative Pipelines / DLT) — Full Guide](#10-databricks-pipelines-lakeflow-declarative-pipelines--dlt)
# MAGIC 11. [Delta Live Tables (DLT) Deep Dive](#11-delta-live-tables-dlt-deep-dive)
# MAGIC 12. [Query Profiling](#12-query-profiling)
# MAGIC 13. [Query Caching](#13-query-caching)
# MAGIC 14. [Query Scheduling](#14-query-scheduling)
# MAGIC 15. [Databricks SQL Alerts](#15-databricks-sql-alerts)
# MAGIC 16. [SQL Query vs SQL File](#16-sql-query-vs-sql-file)
# MAGIC 17. [Databricks ETL Jobs for Orchestration (Lakeflow Jobs)](#17-databricks-etl-jobs-for-orchestration-lakeflow-jobs)
# MAGIC 18. [Rapid-Fire Interview Q&A Bank](#18-rapid-fire-interview-qa-bank)
# MAGIC 19. [Cheat Sheet Summary Tables](#19-cheat-sheet-summary-tables)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. What is a SQL Warehouse
# MAGIC
# MAGIC A **SQL Warehouse** (formerly called a **SQL Endpoint** — renamed in 2023) is a **compute resource** in Databricks that lets you run SQL queries and BI/analytics workloads on data stored in your Lakehouse (Delta Lake tables governed by Unity Catalog).
# MAGIC
# MAGIC Think of it as: **"the engine that powers Databricks SQL"** — the same way a warehouse in Snowflake or a cluster in Redshift powers query execution, a SQL Warehouse in Databricks does that on top of the Lakehouse.
# MAGIC
# MAGIC ### Key facts
# MAGIC - It's a **Spark-based, SQL-optimized cluster** exposed through JDBC/ODBC, the SQL Editor, Catalog Explorer, Dashboards (AI/BI), BI tools (Power BI, Tableau), and REST APIs.
# MAGIC - Most Databricks Lakehouse Platform users only interact with SQL Warehouses that an **admin has already created** — regular users just pick one from a dropdown.
# MAGIC - Creating/configuring a warehouse requires **elevated (admin) permissions**.
# MAGIC - Databricks auto-creates a small **"Starter Warehouse"** for every new workspace so beginners have something to query with immediately.
# MAGIC - **Auto-start** behavior: A stopped warehouse automatically restarts when:
# MAGIC   - A query is run against it
# MAGIC   - A job is scheduled against a stopped warehouse
# MAGIC   - A JDBC/ODBC connection is established
# MAGIC   - A dashboard tied to that warehouse is opened
# MAGIC - **Auto-stop**: Warehouses can be configured to automatically stop after a period of inactivity to save cost.
# MAGIC - Manual start requires **CAN MONITOR** permission at minimum.
# MAGIC - **Governed by Unity Catalog** for data access (fine-grained permissions on catalogs/schemas/tables), though custom data access configs are also possible.
# MAGIC
# MAGIC ### Why does a SQL Warehouse exist (vs. an All-Purpose/Job cluster)?
# MAGIC | All-Purpose / Job Cluster | SQL Warehouse |
# MAGIC |---|---|
# MAGIC | General compute: Python, Scala, R, SQL, ML | SQL-only, BI/analytics-optimized |
# MAGIC | Used inside notebooks/jobs | Used in SQL Editor, dashboards, BI tools, JDBC/ODBC |
# MAGIC | Billed as **Data Engineering / Jobs Compute** | Billed as **Databricks SQL Compute** (different SKU, usually cheaper per query for BI workloads) |
# MAGIC | Manual sizing, less workload-aware autoscaling | Purpose-built autoscaling, intelligent workload management (serverless) |
# MAGIC
# MAGIC ### SQL Warehouse vs SQL Endpoint
# MAGIC These are the **exact same thing**. In 2023 Databricks rebranded "SQL Endpoints" to "**SQL Warehouses**" to better reflect their purpose as a data-warehousing compute layer. If you see "endpoint" in older docs/exams, mentally replace it with "warehouse."
# MAGIC
# MAGIC ### Serverless Compute Plane (short note)
# MAGIC With **serverless SQL warehouses**, the compute plane (the actual VMs executing your query) lives in **Databricks' own cloud account**, not yours. This is what enables near-instant start times (2–6 seconds) because Databricks keeps a warm pool of capacity ready.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Types of SQL Warehouses
# MAGIC
# MAGIC Databricks SQL supports **three main warehouse types**, plus a specialized new type:
# MAGIC
# MAGIC 1. **Serverless**
# MAGIC 2. **Pro**
# MAGIC 3. **Classic**
# MAGIC 4. **Lakehouse Real-Time (Lakehouse//RT)** — Beta, specialized
# MAGIC
# MAGIC ### 2.1 Feature comparison
# MAGIC
# MAGIC | Warehouse type | Photon Engine | Predictive I/O | Intelligent Workload Management (IWM) |
# MAGIC |---|:---:|:---:|:---:|
# MAGIC | **Serverless** | ✅ | ✅ | ✅ |
# MAGIC | **Pro** | ✅ | ✅ | ❌ |
# MAGIC | **Classic** | ✅ | ❌ | ❌ |
# MAGIC
# MAGIC **What each feature means:**
# MAGIC - **Photon**: Databricks' native vectorized query engine (written in C++) that accelerates SQL and DataFrame execution and lowers cost per workload. Available on all three types.
# MAGIC - **Predictive I/O**: A suite of features that speed up **selective scans** (e.g., point lookups, filtered reads) by intelligently deciding what data to read.
# MAGIC - **Intelligent Workload Management (IWM)**: AI/ML-driven system, **exclusive to Serverless**, that predicts query resource needs and dynamically scales clusters — this is the biggest differentiator of serverless.
# MAGIC
# MAGIC ### 2.2 Serverless SQL Warehouses (Recommended default)
# MAGIC
# MAGIC - Compute plane runs in **Databricks' cloud account** — no infra to manage.
# MAGIC - **Startup time: ~2–6 seconds** (vs. ~4 minutes for Pro/Classic).
# MAGIC - Uses **Intelligent Workload Management (IWM)** to scale rapidly up and down based on real-time demand.
# MAGIC - Best price/performance; Databricks actively recommends this as the default choice.
# MAGIC - Best for: **ETL, BI dashboards, exploratory analysis** — basically everything, unless you have a specific reason not to.
# MAGIC - ⚠️ Limitation: SQL Warehouses (of any type) **do not support credential passthrough** — use Unity Catalog for governance instead.
# MAGIC - ⚠️ Not available in every region, and not compatible with legacy external Hive metastores.
# MAGIC
# MAGIC **Advantages of Serverless over Pro/Classic:**
# MAGIC - Instant, elastic compute (no waiting, no over-provisioning)
# MAGIC - Minimal management overhead (Databricks handles patching, upgrades, capacity)
# MAGIC - Lower Total Cost of Ownership (auto-scale down = less idle cost)
# MAGIC
# MAGIC ### 2.3 Pro SQL Warehouses
# MAGIC
# MAGIC - Compute plane lives in **your own cloud subscription** (unlike serverless).
# MAGIC - Supports Photon + Predictive I/O, but **not** IWM → scales more slowly, using the older rule-based autoscaler.
# MAGIC - Startup time: **~4 minutes**.
# MAGIC - **Use when:**
# MAGIC   - Serverless isn't available in your region.
# MAGIC   - You need custom networking (VNet/VPC peering) to connect to on-prem databases or other services (federation / hybrid architecture).
# MAGIC
# MAGIC ### 2.4 Classic SQL Warehouses
# MAGIC
# MAGIC - Compute plane in your own cloud subscription.
# MAGIC - Only supports **Photon** — no Predictive I/O, no IWM.
# MAGIC - Entry-level performance; least advanced tier.
# MAGIC - Startup time: **~4 minutes**.
# MAGIC - **Use when:** You just want basic interactive/exploratory querying and don't need top performance.
# MAGIC
# MAGIC ### 2.5 Lakehouse Real-Time (Lakehouse//RT) — Beta
# MAGIC
# MAGIC - A **specialized serverless warehouse** built for **sub-second, high-concurrency SELECT-only** queries.
# MAGIC - Designed for: serving analytical data to **applications**, powering BI dashboards with **hundreds–thousands of concurrent users**, operational analytics.
# MAGIC - **Only supports SELECT queries** (read-only).
# MAGIC
# MAGIC ### 2.6 Warehouse type defaults (important gotcha for interviews)
# MAGIC
# MAGIC | Creation method | Region supports Serverless? | Default type |
# MAGIC |---|---|---|
# MAGIC | UI | Yes | **Serverless** |
# MAGIC | UI | No | Pro |
# MAGIC | REST API (default params) | Any | **Classic** (you must explicitly set `enable_serverless_compute=true` and `warehouse_type=pro` to get serverless via API) |
# MAGIC | Workspace uses legacy external Hive metastore | N/A | Serverless not supported at all; defaults to Pro (UI) / Classic (API) |
# MAGIC
# MAGIC > 🎯 **Interview trap**: "If I create a warehouse via UI it defaults to serverless, but via API it defaults to classic — unless I explicitly set the serverless flags." This asymmetry is a favorite gotcha question.
# MAGIC
# MAGIC ### 2.7 Classic/Pro cluster sizes (know at least the pattern)
# MAGIC
# MAGIC | Cluster size | Driver | Workers |
# MAGIC |---|---|---|
# MAGIC | 2X-Small | 1 node | 1x node |
# MAGIC | X-Small | 1 driver | 2x nodes |
# MAGIC | Small | bigger driver | 4x nodes |
# MAGIC | Medium | bigger driver | 8x nodes |
# MAGIC | Large | bigger driver | 16x nodes |
# MAGIC | X-Large | biggest driver | 32x nodes |
# MAGIC | 2X-Large | biggest driver | 64x nodes |
# MAGIC | 3X-Large | biggest driver | 128x nodes |
# MAGIC | 4X-Large | biggest driver | 256x nodes |
# MAGIC | 5X-Large (Preview) | biggest driver | 512x nodes |
# MAGIC
# MAGIC Each size **doubles worker count** as you go up a tier. Each pro/classic SKU has a **fixed limit of 1 cluster per 10 concurrent queries** — this is why multi-cluster autoscaling exists (to handle more concurrency, not bigger single queries).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Warehouse Sizing, Scaling & Queuing Behavior
# MAGIC
# MAGIC This is one of the **most commonly asked practical/interview topics**. Split into Serverless vs Classic/Pro because the mechanics are completely different.
# MAGIC
# MAGIC ### 3.1 Serverless: Intelligent Workload Management (IWM)
# MAGIC
# MAGIC IWM uses **machine learning models** to manage compute dynamically:
# MAGIC
# MAGIC 1. A new query arrives.
# MAGIC 2. IWM **predicts its resource needs** and checks current capacity.
# MAGIC    - If capacity exists → query starts **immediately**.
# MAGIC    - If not → query goes into a **queue**.
# MAGIC 3. IWM **continuously monitors the queue**. If wait times grow, the autoscaler **rapidly spins up more clusters**.
# MAGIC 4. When demand drops, IWM **scales down** — but keeps just enough capacity to absorb recent peak load (avoids "cold start churn").
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Rapid upscaling → low latency
# MAGIC - High throughput (queries admitted as soon as hardware is ready)
# MAGIC - Rapid downscaling → cost savings
# MAGIC
# MAGIC ### 3.2 Sizing guidance for Serverless
# MAGIC
# MAGIC - **Start with one larger warehouse** and let IWM manage concurrency — it's usually more efficient to size **down** later than to start small and scale up.
# MAGIC - If you see **query spill to disk** (check the Query Profile), **increase cluster size**.
# MAGIC - For workloads with **many concurrent users**, configure a sufficient **maximum cluster count**, and watch the **"Peak Queued Queries"** metric on the monitoring page.
# MAGIC
# MAGIC ### 3.3 Statement Timeout (Beta feature)
# MAGIC
# MAGIC - You can configure a **`statement_timeout`** at the **warehouse level** via the Warehouse Create/Update REST API, so long-running queries auto-cancel after a set duration.
# MAGIC - **Precedence order** (highest wins): **Session > Warehouse > Workspace**. So a session-level timeout overrides a warehouse-level one, which overrides a workspace-wide default.
# MAGIC
# MAGIC ### 3.4 Monitoring tools (for right-sizing any warehouse)
# MAGIC
# MAGIC | Tool | What it tells you |
# MAGIC |---|---|
# MAGIC | **Monitoring page → Peak Queued Queries** | Consistent value > 0 = you need a bigger cluster or more clusters |
# MAGIC | **Query History** | Review historical run times, spot slow patterns |
# MAGIC | **Query Profile → Bytes Spilled to Disk** | Warehouse is undersized (memory pressure) |
# MAGIC
# MAGIC > The maximum queue length for **any** warehouse type is **1,000 queries**.
# MAGIC
# MAGIC ### 3.5 Classic / Pro: Manual Scaling Model
# MAGIC
# MAGIC Unlike serverless, you **manually configure**:
# MAGIC - The **cluster size** (X-Small → 5X-Large)
# MAGIC - **Min clusters** and **Max clusters** (multi-cluster load balancing / autoscaling range)
# MAGIC
# MAGIC Each cluster size SKU can handle a **fixed ~10 concurrent queries per cluster**. To handle more concurrency, Databricks spins up **additional clusters of the same size** (not bigger clusters) — this is called **multi-cluster load balancing**.
# MAGIC
# MAGIC ### 3.6 Classic/Pro Queuing & Autoscaling Logic (memorize this table — very commonly quizzed!)
# MAGIC
# MAGIC Autoscaling decides **how many new clusters to add** based on the **estimated total time to clear the queue + running queries**:
# MAGIC
# MAGIC | Estimated time to process running + queued queries | Clusters added |
# MAGIC |---|---|
# MAGIC | 2–6 minutes | +1 cluster |
# MAGIC | 6–12 minutes | +2 clusters |
# MAGIC | 12–22 minutes | +3 clusters |
# MAGIC | More than 22 minutes | +3 clusters, then +1 more for every additional 15 minutes of load |
# MAGIC
# MAGIC **Additional trigger rules:**
# MAGIC - If a query sits in queue for **5 minutes**, the warehouse **scales up** immediately (regardless of the table above).
# MAGIC - If load stays low for **15 consecutive minutes**, the warehouse **scales down** to just enough clusters to handle the peak load seen in that window (not to zero, unless auto-stop kicks in separately).
# MAGIC
# MAGIC ### 3.7 Required Cloud vCPU Quota (Azure-specific but conceptually universal)
# MAGIC
# MAGIC To start a Classic/Pro warehouse you need enough vCPU quota in your cloud account:
# MAGIC - For 1–2 warehouses: reserve **~8 vCPUs per core** in the cluster (covers periodic re-provisioning, which happens roughly every 24 hours).
# MAGIC - As the number of warehouses grows: reserve **4–8 vCPUs per core**, and monitor stability.
# MAGIC - This quota is **in addition to** vCPUs used by other clusters (Data Engineering, ML, etc.)
# MAGIC
# MAGIC ### 3.8 Serverless vs Classic/Pro — Scaling Summary Table
# MAGIC
# MAGIC | Aspect | Serverless (IWM) | Classic/Pro (Manual) |
# MAGIC |---|---|---|
# MAGIC | Scaling driver | ML-based prediction | Rule-based (queue-time thresholds) |
# MAGIC | Startup time | 2–6 sec | ~4 min |
# MAGIC | Scale-up trigger | Predicted congestion | Estimated wait time buckets + 5-min queue rule |
# MAGIC | Scale-down trigger | Demand drop (keeps peak buffer) | 15 min of sustained low load |
# MAGIC | Granularity | Dynamic, workload-aware | Fixed cluster sizes, +1 cluster per threshold |
# MAGIC | You configure | Warehouse size + max clusters | Cluster size + min/max clusters |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Query Parameters
# MAGIC
# MAGIC **Query parameters** let you turn hard-coded literal values in a SQL query into **dynamic, user-editable inputs** — essential for reusable dashboards, ad hoc analysis, and parameterized reports.
# MAGIC
# MAGIC ### 4.1 Syntax
# MAGIC
# MAGIC Insert a parameter with a **colon + name**:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM samples.nyctaxi.trips
# MAGIC WHERE fare_amount < :fare_parameter
# MAGIC ```
# MAGIC
# MAGIC The moment you type `:fare_parameter` in the SQL Editor, Databricks automatically renders an **interactive widget** above/beside the editor where users can enter or change the value — without touching the SQL text.
# MAGIC
# MAGIC ### 4.2 Parameter Types (configurable via the gear/settings icon on the widget)
# MAGIC
# MAGIC | Type | Behavior |
# MAGIC |---|---|
# MAGIC | **Text** | Free-form string input |
# MAGIC | **Number** | Numeric input |
# MAGIC | **Date / Date & Time** | Date picker |
# MAGIC | **Dropdown List** | Fixed list of values you define |
# MAGIC | **Query-Based Dropdown List** | Populates options dynamically from the result of **another saved query** |
# MAGIC
# MAGIC **Query-based dropdown nuance (interview favorite):**
# MAGIC - If the source query returns **>1 column**, Databricks by default uses only the **first column**.
# MAGIC - If the source query returns a `name` and `value` column pair, the **widget displays `name`** to the user but **executes the query using the corresponding `value`**. E.g., a dropdown showing "Region A" but silently passing `1001` into the query.
# MAGIC - Databricks warns that large result sets from the dropdown source query will **degrade performance**.
# MAGIC
# MAGIC ### 4.3 Multi-value / Range Parameters
# MAGIC Databricks SQL parameters support selecting **multiple values in a single query** and parameterizing **rollups** by day/month/year — useful for dashboards where a user picks several regions or a date range.
# MAGIC
# MAGIC ### 4.4 Where Parameters Work
# MAGIC Parameters are supported consistently across:
# MAGIC - SQL Editor (Databricks SQL)
# MAGIC - Notebooks
# MAGIC - Dashboards (AI/BI)
# MAGIC - Lakeflow Jobs
# MAGIC - SQL Execution REST API
# MAGIC - Via connector drivers (e.g., `databricks-sql-python`) using `:param_name` or `%(param)s`/`%s` (legacy inline style) — the modern connector uses native parameter binding, which also **prevents SQL injection** and can **improve performance** (query plans can be reused).
# MAGIC
# MAGIC ### 4.5 Common troubleshooting (real interview scenario)
# MAGIC A frequent real-world bug: parameters don't work if you're **not inside the actual SQL Editor context** (e.g., pasting SQL somewhere else), or spacing/quoting around `{{ }}` (older parameter syntax) is wrong. Modern syntax uses `:param_name` (no double curly braces needed).
# MAGIC
# MAGIC ### 4.6 Why Query Parameters matter (interview answer)
# MAGIC - Avoids duplicating near-identical queries (`WHERE region = 'US'` vs `'EU'` etc.)
# MAGIC - Powers **self-service BI**: business users can adjust filters without SQL knowledge
# MAGIC - Enables **safe, reusable, parameterized ETL/reporting queries**
# MAGIC - Works hand-in-hand with **Dashboards**, where each viewer can supply their own parameter values
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Query Snippets
# MAGIC
# MAGIC **Query snippets** are **reusable blocks of SQL text** (like code snippets in an IDE) that you can insert into any query with a shortcut, saving you from retyping common WHERE clauses, CTEs, or boilerplate.
# MAGIC
# MAGIC ### 5.1 How to create one
# MAGIC 1. Open the **SQL Editor** (or Notebook/File editor).
# MAGIC 2. Click the **kebab menu (⋮) → View → Query snippets**.
# MAGIC 3. Click **Create query snippet**.
# MAGIC 4. Fill in:
# MAGIC    - **Replace field** → the *trigger name* you'll type to invoke it.
# MAGIC    - **Snippet field** → the actual SQL text to insert.
# MAGIC 5. Click **Create**.
# MAGIC
# MAGIC ### 5.2 How to use one
# MAGIC - In the SQL editor, start typing the **first 3 letters** of the snippet name → an autocomplete window shows it → select it.
# MAGIC - Alternatively, press **Ctrl+Space** to manually open the snippet picker.
# MAGIC - Snippets support **insertion points** — placeholders you tab through and fill in (similar to VS Code snippets), optionally with **default values**, and you can have **multiple insertion points** in one snippet.
# MAGIC
# MAGIC ### 5.3 Where snippets are available
# MAGIC - New SQL Editor
# MAGIC - Notebook SQL cells
# MAGIC - SQL files
# MAGIC - AI/BI Dashboard queries
# MAGIC
# MAGIC ### 5.4 Why this matters (interview answer)
# MAGIC - Boosts productivity for repetitive boilerplate (standard filters, common joins, company-specific business logic like "active customers" definitions).
# MAGIC - Encourages **standardization** across a team (e.g., a snippet enforcing a specific date-filter pattern that everyone should use).
# MAGIC - Different from **Query Parameters**: parameters make ONE query dynamic; snippets let you **reuse chunks of SQL text across MANY different queries**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Streaming Tables
# MAGIC
# MAGIC ### 6.1 What is a Streaming Table?
# MAGIC
# MAGIC A **Streaming Table** is a Delta table designed for **incremental / streaming ingestion** — each time it's refreshed, only **newly arrived data** from the source is processed and appended, instead of reprocessing everything. Streaming tables are always **backed by a pipeline** (Lakeflow Declarative Pipelines, formerly DLT).
# MAGIC
# MAGIC - Refreshed **manually** or **on a schedule**.
# MAGIC - Ideal for the **Bronze layer** (raw ingestion) and incremental **Silver** transformations in a Medallion architecture.
# MAGIC
# MAGIC ### 6.2 Syntax
# MAGIC
# MAGIC ```sql
# MAGIC CREATE [OR REFRESH] [PRIVATE] STREAMING TABLE table_name
# MAGIC   [ table_specification ]
# MAGIC   [ table_clauses ]
# MAGIC   [ {flow_clause | AS query} ]
# MAGIC ```
# MAGIC
# MAGIC ### 6.3 Simple Examples
# MAGIC
# MAGIC ```sql
# MAGIC -- From a volume of files
# MAGIC CREATE OR REFRESH STREAMING TABLE customers_bronze
# MAGIC AS SELECT * FROM STREAM read_files("/databricks-datasets/retail-org/customers/*", format => "csv");
# MAGIC
# MAGIC -- From another streaming table
# MAGIC CREATE OR REFRESH STREAMING TABLE customers_silver
# MAGIC AS SELECT * FROM STREAM(customers_bronze);
# MAGIC
# MAGIC -- Using automatic Liquid Clustering
# MAGIC CREATE OR REFRESH STREAMING TABLE customers_bronze_auto
# MAGIC CLUSTER BY AUTO
# MAGIC AS SELECT * FROM STREAM read_files("/databricks-datasets/retail-org/customers/*", format => "csv");
# MAGIC ```
# MAGIC
# MAGIC ### 6.4 Key Clauses
# MAGIC
# MAGIC | Clause | Purpose |
# MAGIC |---|---|
# MAGIC | `OR REFRESH` | Create the table, or refresh an existing one's content/definition |
# MAGIC | `PRIVATE` | Table not registered in the catalog; only visible **within the defining pipeline**; can shadow a catalog object of the same name; lives only for the pipeline's lifetime |
# MAGIC | `USING DELTA` | Only supported format (default anyway) |
# MAGIC | `PARTITIONED BY` | Classic Hive-style partitioning — **mutually exclusive** with `CLUSTER BY` |
# MAGIC | `CLUSTER BY` / `CLUSTER BY AUTO` | Enables **Liquid Clustering** (Databricks' recommended modern alternative to partitioning); `AUTO` lets Databricks pick clustering keys itself |
# MAGIC | `LOCATION` | Custom storage path (defaults to pipeline's storage location) |
# MAGIC | `WITH ROW FILTER` | Applies a row-level security function |
# MAGIC | Column `MASK` clause | Applies a column-masking function (data anonymization, e.g., masking SSNs) |
# MAGIC | `FLOW` | Explicitly define how data flows into the table (see below) |
# MAGIC
# MAGIC ### 6.5 Flow Types
# MAGIC
# MAGIC ```
# MAGIC FLOW { INSERT [ONCE] BY NAME query
# MAGIC      | AUTO CDC auto_cdc_flow_spec
# MAGIC      | REPLACE WHERE predicate BY NAME query }
# MAGIC ```
# MAGIC
# MAGIC - **`INSERT BY NAME`** — Standard append flow. Equivalent to just using `AS query`. Must be a streaming source (`STREAM read_files(...)` etc.) unless `ONCE` is used.
# MAGIC - **`ONCE`** — A **one-time flow** (e.g., a backfill). Runs once; re-runs only if you do a **full refresh**.
# MAGIC - **`AUTO CDC`** — Processes **change-data-capture** records (inserts/updates/deletes) into the table using SCD Type 1 or Type 2 logic. (Full section below.)
# MAGIC - **`REPLACE WHERE`** (Beta) — Recomputes/overwrites only rows matching a predicate; great for late-arriving data, backfills, incremental batch aggregations.
# MAGIC
# MAGIC ### 6.6 Read Options (Databricks Runtime 17.3+)
# MAGIC
# MAGIC You can pass options like:
# MAGIC ```sql
# MAGIC SELECT * FROM STREAM source_table WITH (SKIPCHANGECOMMITS=TRUE, STARTINGVERSION=X)
# MAGIC ```
# MAGIC Supported: `maxFilesPerTrigger`, `maxBytesPerTrigger`, `startingVersion`, `startingTimestamp`, `readChangeFeed`, `withEventTimeOrder`, `skipChangeCommits`.
# MAGIC
# MAGIC ### 6.7 Important Limitations (frequently tested!)
# MAGIC
# MAGIC - Only the **table owner** can refresh a streaming table.
# MAGIC - `ALTER TABLE` is **not allowed** — you must use `CREATE OR REFRESH` or `ALTER STREAMING TABLE`.
# MAGIC - Cannot evolve schema via `INSERT INTO` / `MERGE`.
# MAGIC - **Not supported**: `CLONE`, `COPY INTO`, `ANALYZE TABLE`, `RESTORE`, `TRUNCATE`, `GENERATE MANIFEST`, `[CREATE OR] REPLACE TABLE`.
# MAGIC - Cannot rename the table or change its owner.
# MAGIC - No generated columns, identity columns, or default columns.
# MAGIC
# MAGIC ### 6.8 Required Permissions
# MAGIC - To create: `SELECT` on source tables, `USE CATALOG`/`USE SCHEMA` on target, `CREATE MATERIALIZED VIEW` on target schema (yes — streaming tables share creation privilege with materialized views).
# MAGIC - To refresh/update pipeline: ownership or `REFRESH` privilege, plus the owner needs `SELECT` on the base tables.
# MAGIC - To query: `USE CATALOG`/`USE SCHEMA` + `SELECT` on the streaming table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7. SCD Type 1
# MAGIC
# MAGIC **Slowly Changing Dimension Type 1 (SCD1)** = **overwrite the old value with the new value**. No history is kept — the table always reflects only the **current/latest state**.
# MAGIC
# MAGIC ### 7.1 Concept Example
# MAGIC
# MAGIC | Before | After city update |
# MAGIC |---|---|
# MAGIC | CustomerID=125, City=Tijuana | CustomerID=125, City=**Guadalajara** (Tijuana is gone forever) |
# MAGIC
# MAGIC ### 7.2 When to use SCD1
# MAGIC - When historical values **don't matter** for the business use case (e.g., correcting a typo in a customer's name/email).
# MAGIC - Simpler, smaller storage footprint, faster queries (no duplicate historical rows).
# MAGIC
# MAGIC ### 7.3 Implementing SCD1 manually (classic `MERGE` approach — very common interview question!)
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO target_customers AS t
# MAGIC USING source_customers AS s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET t.name = s.name, t.city = s.city, t.updated_at = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (customer_id, name, city, updated_at)
# MAGIC   VALUES (s.customer_id, s.name, s.city, current_timestamp());
# MAGIC ```
# MAGIC
# MAGIC ### 7.4 Implementing SCD1 with Lakeflow Pipelines `AUTO CDC` (modern, declarative approach)
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE users_current;
# MAGIC
# MAGIC CREATE FLOW apply_cdc AS AUTO CDC INTO
# MAGIC   users_current
# MAGIC FROM
# MAGIC   stream(main.cdc_tutorial.users_cdf)
# MAGIC KEYS
# MAGIC   (userId)
# MAGIC APPLY AS DELETE WHEN
# MAGIC   operation = "DELETE"
# MAGIC APPLY AS TRUNCATE WHEN
# MAGIC   operation = "TRUNCATE"
# MAGIC SEQUENCE BY
# MAGIC   sequenceNum
# MAGIC COLUMNS * EXCEPT
# MAGIC   (operation, sequenceNum)
# MAGIC STORED AS
# MAGIC   SCD TYPE 1;
# MAGIC ```
# MAGIC
# MAGIC **Result:** only latest state per `userId` is kept. Out-of-order updates are correctly handled because of `SEQUENCE BY` — an older update arriving late is automatically dropped in favor of the already-applied newer one.
# MAGIC
# MAGIC ### 7.5 SCD1 Pros/Cons
# MAGIC
# MAGIC | Pros | Cons |
# MAGIC |---|---|
# MAGIC | Simple to implement | No audit trail / history |
# MAGIC | Smaller table size | Can't answer "what was the value on date X?" |
# MAGIC | Fast to query (no need to filter for "current" row) | Not suitable for regulatory/compliance needs requiring history |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 8. SCD Type 2
# MAGIC
# MAGIC **Slowly Changing Dimension Type 2 (SCD2)** = **preserve full history** by inserting a **new row** for every change, and marking validity periods using `__START_AT` / `__END_AT` (or similarly named `effective_date`/`end_date`) columns.
# MAGIC
# MAGIC ### 8.1 Concept Example
# MAGIC
# MAGIC | userId | name | city | __START_AT | __END_AT |
# MAGIC |---|---|---|---|---|
# MAGIC | 125 | Mercedes | Tijuana | 2 | 5 |
# MAGIC | 125 | Mercedes | Mexicali | 5 | 6 |
# MAGIC | 125 | Mercedes | Guadalajara | 6 | **null (current)** |
# MAGIC
# MAGIC A row with `__END_AT = null` is the **currently active** version.
# MAGIC
# MAGIC ### 8.2 SCD2 via `AUTO CDC`
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE users_history;
# MAGIC
# MAGIC CREATE FLOW apply_cdc AS AUTO CDC INTO
# MAGIC   users_history
# MAGIC FROM
# MAGIC   stream(main.cdc_tutorial.users_cdf)
# MAGIC KEYS
# MAGIC   (userId)
# MAGIC APPLY AS DELETE WHEN
# MAGIC   operation = "DELETE"
# MAGIC SEQUENCE BY
# MAGIC   sequenceNum
# MAGIC COLUMNS * EXCEPT
# MAGIC   (operation, sequenceNum)
# MAGIC STORED AS
# MAGIC   SCD TYPE 2;
# MAGIC ```
# MAGIC
# MAGIC Behavior: **any column change** by default triggers a new history row.
# MAGIC
# MAGIC ### 8.3 Tracking only a subset of columns (`TRACK HISTORY ON`)
# MAGIC
# MAGIC Sometimes you only want history when *specific* columns change (e.g., track address changes but not "last_login_time"):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE users_history;
# MAGIC
# MAGIC CREATE FLOW apply_cdc AS AUTO CDC INTO
# MAGIC   users_history
# MAGIC FROM
# MAGIC   stream(main.cdc_tutorial.users_cdf)
# MAGIC KEYS
# MAGIC   (userId)
# MAGIC APPLY AS DELETE WHEN
# MAGIC   operation = "DELETE"
# MAGIC SEQUENCE BY
# MAGIC   sequenceNum
# MAGIC COLUMNS * EXCEPT
# MAGIC   (operation, sequenceNum)
# MAGIC STORED AS
# MAGIC   SCD TYPE 2
# MAGIC TRACK HISTORY ON * EXCEPT
# MAGIC   (city);
# MAGIC ```
# MAGIC
# MAGIC Here, changes to `city` will **overwrite the current row in place** instead of creating a new version, because `city` is excluded from history tracking.
# MAGIC
# MAGIC ### 8.4 Manual SCD2 with `MERGE` (interview classic — you must be able to write this from scratch)
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Close out changed records (set end_date, is_current = false)
# MAGIC MERGE INTO dim_customer AS t
# MAGIC USING staged_customer AS s
# MAGIC ON t.customer_id = s.customer_id AND t.is_current = true
# MAGIC WHEN MATCHED AND (t.city <> s.city OR t.name <> s.name) THEN
# MAGIC   UPDATE SET t.end_date = current_date(), t.is_current = false;
# MAGIC
# MAGIC -- Step 2: Insert new versions for changed + brand-new records
# MAGIC INSERT INTO dim_customer (customer_id, name, city, start_date, end_date, is_current)
# MAGIC SELECT s.customer_id, s.name, s.city, current_date(), NULL, true
# MAGIC FROM staged_customer s
# MAGIC LEFT JOIN dim_customer t
# MAGIC   ON s.customer_id = t.customer_id AND t.is_current = true
# MAGIC WHERE t.customer_id IS NULL
# MAGIC    OR t.city <> s.city OR t.name <> s.name;
# MAGIC ```
# MAGIC
# MAGIC ### 8.5 SCD1 vs SCD2 — head-to-head (guaranteed interview question)
# MAGIC
# MAGIC | Aspect | SCD Type 1 | SCD Type 2 |
# MAGIC |---|---|---|
# MAGIC | History | ❌ None (overwrite) | ✅ Full history retained |
# MAGIC | Storage | Low | Higher (new row per change) |
# MAGIC | Query complexity | Simple | Must filter `WHERE __END_AT IS NULL` (or `is_current=true`) for current state |
# MAGIC | Use case | Fixing errors, non-critical attribute updates | Auditing, "as-of" reporting, regulatory compliance, tracking customer journeys |
# MAGIC | Implementation in Databricks | `AUTO CDC ... STORED AS SCD TYPE 1` or `MERGE` | `AUTO CDC ... STORED AS SCD TYPE 2` or multi-step `MERGE`+`INSERT` |
# MAGIC | Bitemporal support | N/A | AUTO CDC (Beta) extends SCD2 to **bitemporal** tracking (business time + system time) |
# MAGIC
# MAGIC ### 8.6 SCD Type 3 (bonus — sometimes asked)
# MAGIC Stores only **limited history** by adding extra columns like `previous_city`, `current_city` — a middle ground rarely used in modern lakehouses (superseded by SCD2 + time travel).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 9. AUTO CDC in Databricks
# MAGIC
# MAGIC ### 9.1 What is AUTO CDC?
# MAGIC
# MAGIC **`AUTO CDC`** (and **`AUTO CDC FROM SNAPSHOT`**) are Lakeflow Pipeline APIs that **automate the complexity of computing SCD Type 1 / Type 2 tables** from either:
# MAGIC - A genuine **Change Data Feed (CDC feed)** from the source (`AUTO CDC`), or
# MAGIC - Periodic **snapshots** when the source has no CDC feed (`AUTO CDC FROM SNAPSHOT`).
# MAGIC
# MAGIC > 🔑 **Naming history (common trivia question):** `AUTO CDC` **replaces** the older `APPLY CHANGES` API. Same syntax, new name. `APPLY CHANGES` still works but Databricks recommends migrating to `AUTO CDC`.
# MAGIC
# MAGIC ### 9.2 AUTO CDC vs AUTO CDC FROM SNAPSHOT
# MAGIC
# MAGIC | | `AUTO CDC` | `AUTO CDC FROM SNAPSHOT` |
# MAGIC |---|---|---|
# MAGIC | Source | A CDC feed already exists (e.g., Debezium, CDF-enabled DB) | No CDC feed — only periodic full/partial snapshots available |
# MAGIC | How it detects changes | Reads pre-existing change events (`INSERT`/`UPDATE`/`DELETE` operations) | Compares two ordered snapshots to *infer* what changed |
# MAGIC | Interfaces supported | SQL **and** Python | **Python only** |
# MAGIC | Bitemporal support | ✅ (Beta) | ❌ |
# MAGIC
# MAGIC ### 9.3 Requirements
# MAGIC - Pipeline must be **serverless Lakeflow Pipelines** or **Pro/Advanced edition** of classic Lakeflow Pipelines.
# MAGIC - `AUTO CDC` is **not supported** by open-source Apache Spark Declarative Pipelines — it's a Databricks-only capability.
# MAGIC
# MAGIC ### 9.4 How AUTO CDC works — step by step
# MAGIC
# MAGIC 1. Create a target **streaming table**: `CREATE OR REFRESH STREAMING TABLE users_current;`
# MAGIC 2. Define a flow using `AUTO CDC INTO` (SQL) or `create_auto_cdc_flow()` (Python).
# MAGIC 3. Specify:
# MAGIC    - **`KEYS`** — the primary key(s) to match records.
# MAGIC    - **`SEQUENCE BY`** — the column that determines ordering (e.g., a timestamp or version number). Handles **out-of-order events** correctly.
# MAGIC    - **`APPLY AS DELETE WHEN`** — condition marking a record for deletion.
# MAGIC    - **`APPLY AS TRUNCATE WHEN`** — condition marking a full-table truncate.
# MAGIC    - **`COLUMNS * EXCEPT (...)`** — which columns to carry into the target (excluding CDC metadata columns like `operation`, `sequenceNum`).
# MAGIC    - **`STORED AS SCD TYPE 1 | 2`** — output modeling.
# MAGIC
# MAGIC ### 9.5 Full Worked SQL Example
# MAGIC
# MAGIC **Sample CDC source data (`main.cdc_tutorial.users_cdf`):**
# MAGIC
# MAGIC | userId | name | city | operation | sequenceNum |
# MAGIC |---|---|---|---|---|
# MAGIC | 124 | Raul | Oaxaca | INSERT | 1 |
# MAGIC | 123 | Isabel | Monterrey | INSERT | 1 |
# MAGIC | 125 | Mercedes | Tijuana | INSERT | 2 |
# MAGIC | 123 | null | null | DELETE | 6 |
# MAGIC | 125 | Mercedes | Guadalajara | UPDATE | 6 |
# MAGIC | 125 | Mercedes | Mexicali | UPDATE | 5 *(arrives out of order!)* |
# MAGIC
# MAGIC **SCD Type 1 flow:**
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE users_current;
# MAGIC
# MAGIC CREATE FLOW apply_cdc AS AUTO CDC INTO users_current
# MAGIC FROM stream(main.cdc_tutorial.users_cdf)
# MAGIC KEYS (userId)
# MAGIC APPLY AS DELETE WHEN operation = "DELETE"
# MAGIC APPLY AS TRUNCATE WHEN operation = "TRUNCATE"
# MAGIC SEQUENCE BY sequenceNum
# MAGIC COLUMNS * EXCEPT (operation, sequenceNum)
# MAGIC STORED AS SCD TYPE 1;
# MAGIC ```
# MAGIC **Result:** user 123 is gone (deleted). User 125 shows only `Guadalajara` — even though `Mexicali` (seq 5) physically arrived *after* `Guadalajara` (seq 6) in processing order, `SEQUENCE BY` correctly discards it because its sequence number is **older**.
# MAGIC
# MAGIC **SCD Type 2 flow** (same source, different target):
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE users_history;
# MAGIC
# MAGIC CREATE FLOW apply_cdc AS AUTO CDC INTO users_history
# MAGIC FROM stream(main.cdc_tutorial.users_cdf)
# MAGIC KEYS (userId)
# MAGIC APPLY AS DELETE WHEN operation = "DELETE"
# MAGIC SEQUENCE BY sequenceNum
# MAGIC COLUMNS * EXCEPT (operation, sequenceNum)
# MAGIC STORED AS SCD TYPE 2;
# MAGIC ```
# MAGIC **Result:** full history preserved with `__START_AT` / `__END_AT` per version, e.g., user 125 has 3 rows (Tijuana → Mexicali → Guadalajara).
# MAGIC
# MAGIC ### 9.6 Multi-column Sequencing
# MAGIC When one column isn't enough to break ties (e.g., need timestamp **and** an ID), combine them:
# MAGIC ```sql
# MAGIC SEQUENCE BY STRUCT(timestamp_col, id_col)
# MAGIC ```
# MAGIC Python equivalent: `sequence_by = struct("timestamp_col", "id_col")`.
# MAGIC
# MAGIC ### 9.7 AUTO CDC FROM SNAPSHOT (Python only)
# MAGIC
# MAGIC Two ingestion patterns:
# MAGIC 1. **Pipeline ingestion-time versioning** — a new snapshot is read every pipeline run; version = run timestamp automatically.
# MAGIC    ```python
# MAGIC    dp.create_streaming_table("target")
# MAGIC    dp.create_auto_cdc_from_snapshot_flow(
# MAGIC        target="target", source="source", keys=["userId"], stored_as_scd_type=2
# MAGIC    )
# MAGIC    ```
# MAGIC 2. **Explicit version function** — you write a Python function `next_snapshot_and_version()` that returns `(dataframe, version)` tuples in ascending order; useful when snapshots may arrive **out of order** or multiple arrive at once.
# MAGIC
# MAGIC ### 9.8 Partial Updates & Bitemporal (know these exist)
# MAGIC - **Partial updates**: `AUTO CDC` can apply a change record that updates only a *subset* of columns, leaving others untouched.
# MAGIC - **Bitemporal tracking (Beta, `AUTO CDC` only)**: extends SCD2 to track two independent time dimensions — **business/valid time** (when a fact was true in the real world) and **system/transaction time** (when it was recorded in the database). Useful for financial/regulatory systems needing "as it was known then" vs "as it actually was" reporting.
# MAGIC
# MAGIC ### 9.9 Key Limitations
# MAGIC - Sequencing column must be **sortable**; `NULL` sequence values are **not supported**.
# MAGIC - `AUTO CDC FROM SNAPSHOT` = **Python-only**, no SQL support.
# MAGIC - To stream data *out of* an AUTO CDC target, read its **Change Data Feed (CDF)**, not the table directly via a normal streaming read.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 10. Databricks Pipelines (Lakeflow Declarative Pipelines / DLT)
# MAGIC
# MAGIC > **Naming note:** "Delta Live Tables (DLT)" was **rebranded to "Lakeflow Declarative Pipelines."** They are the same underlying technology — interviewers may use either name interchangeably. This guide uses "pipelines" as the umbrella term.
# MAGIC
# MAGIC ### 10.1 What is a Databricks Pipeline?
# MAGIC
# MAGIC A **pipeline** is a **declarative** way to build ETL: instead of writing imperative step-by-step Spark jobs, you **declare** the tables/views you want and the queries that produce them, and Databricks:
# MAGIC - Automatically figures out the **execution order** (dependency graph / DAG) between your tables.
# MAGIC - Manages **incremental processing** for streaming tables.
# MAGIC - Handles **cluster provisioning, retries, and orchestration** of the underlying flows.
# MAGIC - Tracks **data quality** via *expectations* (constraints).
# MAGIC - Provides built-in **monitoring, lineage, and event logs**.
# MAGIC
# MAGIC ### 10.2 Core Building Blocks
# MAGIC
# MAGIC | Object | What it is |
# MAGIC |---|---|
# MAGIC | **Streaming Table** | Incrementally processed Delta table (see Section 6) |
# MAGIC | **Materialized View** | Precomputed view, refreshed on a schedule; good for aggregations/joins that are expensive to compute live |
# MAGIC | **View** | Non-materialized, always recomputed on query (like a normal SQL view but inside the pipeline graph) |
# MAGIC | **Flow** | The actual data movement logic feeding a table (`INSERT BY NAME`, `AUTO CDC`, `REPLACE WHERE`, etc.) |
# MAGIC
# MAGIC ### 10.3 How to Create a Pipeline (step by step — the practical "how to" you asked for)
# MAGIC
# MAGIC 1. **In the Databricks workspace**, go to **Workflows → Pipelines** (or **Jobs & Pipelines → Create Pipeline**).
# MAGIC 2. Choose **Lakeflow Declarative Pipelines (ETL Pipeline)**.
# MAGIC 3. Configure:
# MAGIC    - **Pipeline name**
# MAGIC    - **Source code**: point to a **notebook or a folder of `.sql`/`.py` files** (transformations folder) that contain your `CREATE STREAMING TABLE`, `CREATE MATERIALIZED VIEW`, `CREATE FLOW` statements.
# MAGIC    - **Destination**: target **catalog** and **schema** (Unity Catalog) where the output tables will be registered.
# MAGIC    - **Pipeline mode**:
# MAGIC      - **Triggered** — runs once per invocation, processes new data, then **stops** (batch-like, cost-efficient).
# MAGIC      - **Continuous** — runs **continuously**, processing new data as it arrives with minimal latency (true streaming).
# MAGIC    - **Compute**: choose **Serverless** (recommended) or classic cluster with configured autoscaling min/max workers.
# MAGIC    - **Edition**: **Core / Pro / Advanced** — features like `AUTO CDC`, expectations enforcement levels, etc. require Pro/Advanced.
# MAGIC 4. **Write your transformation logic** as a set of `CREATE STREAMING TABLE`/`CREATE MATERIALIZED VIEW` statements across bronze → silver → gold layers.
# MAGIC 5. Click **Start** / **Run** to trigger an update, or attach a **schedule** so it runs automatically (e.g., hourly).
# MAGIC 6. Monitor via the **Pipeline graph (DAG) UI** — shows each table as a node, with live status (running, succeeded, failed) and row counts.
# MAGIC
# MAGIC ### 10.4 What you "do" inside a pipeline (the transformation code)
# MAGIC
# MAGIC Typical **medallion architecture** pattern:
# MAGIC
# MAGIC ```sql
# MAGIC -- BRONZE: raw ingestion (streaming table)
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC AS SELECT * FROM STREAM read_files('/Volumes/raw/orders/', format => 'json');
# MAGIC
# MAGIC -- SILVER: cleaned + deduplicated
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_orders
# MAGIC (
# MAGIC   CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC AS SELECT DISTINCT * FROM STREAM(bronze_orders);
# MAGIC
# MAGIC -- GOLD: business aggregation (materialized view)
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
# MAGIC AS SELECT order_date, SUM(amount) AS total_revenue
# MAGIC FROM silver_orders
# MAGIC GROUP BY order_date;
# MAGIC ```
# MAGIC
# MAGIC ### 10.5 Data Quality — Expectations (constraints)
# MAGIC
# MAGIC ```sql
# MAGIC CONSTRAINT valid_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC ```
# MAGIC
# MAGIC | Violation action | Behavior |
# MAGIC |---|---|
# MAGIC | `EXPECT` (default) | Log violation but **keep** the row |
# MAGIC | `ON VIOLATION DROP ROW` | Drop only the bad row |
# MAGIC | `ON VIOLATION FAIL UPDATE` | **Fail the entire pipeline run** if any row violates |
# MAGIC
# MAGIC ### 10.6 Run Modes & Triggering
# MAGIC
# MAGIC | Trigger Type | Description |
# MAGIC |---|---|
# MAGIC | **Manual** | Click "Start" in UI, or call the API |
# MAGIC | **Scheduled** | Attach a cron-like schedule directly on the pipeline |
# MAGIC | **As a Job Task** | Add a "Pipeline" task inside a **Lakeflow Job**, chained with other tasks (this is the standard production orchestration pattern) |
# MAGIC | **Continuous mode** | Pipeline keeps running, processing micro-batches as data lands |
# MAGIC
# MAGIC ### 10.7 Why use Pipelines instead of hand-written notebooks/jobs?
# MAGIC - **Automatic dependency resolution** — you don't write `.trigger()` chains; Databricks builds the DAG from your `SELECT` references.
# MAGIC - **Built-in incrementalism** — streaming tables only process new data, no manual checkpoint/watermark management.
# MAGIC - **Declarative CDC** via `AUTO CDC` — huge amount of boilerplate MERGE logic eliminated.
# MAGIC - **Automatic data quality enforcement** via expectations.
# MAGIC - **Unified monitoring** — one graph shows the health of your entire ETL pipeline, not scattered notebook logs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 11. Delta Live Tables (DLT) Deep Dive
# MAGIC
# MAGIC *(Same technology as Section 10, but here's the DLT-specific vocabulary/answers interviewers expect, since many job postings and questions still say "DLT.")*
# MAGIC
# MAGIC ### 11.1 DLT = the original name for what's now "Lakeflow Declarative Pipelines"
# MAGIC Functionally identical. If asked "What is DLT?" in an interview, answer:
# MAGIC > "Delta Live Tables (now called Lakeflow Declarative Pipelines) is Databricks' framework for building reliable, maintainable, and testable ETL pipelines declaratively — you define *what* the tables should contain, and DLT handles orchestration, incremental processing, retries, and data quality."
# MAGIC
# MAGIC ### 11.2 DLT Pipeline Editions
# MAGIC | Edition | Capabilities |
# MAGIC |---|---|
# MAGIC | **Core** | Basic streaming tables/materialized views |
# MAGIC | **Pro** | + `AUTO CDC`/CDC support |
# MAGIC | **Advanced** | + Data quality enforcement (expectations with `FAIL UPDATE`), advanced governance features |
# MAGIC
# MAGIC ### 11.3 DLT Key Concepts Recap
# MAGIC - **Live Tables (now Materialized Views)**: results are precomputed and stored; refreshed based on pipeline run, not on every query.
# MAGIC - **Streaming Live Tables (now Streaming Tables)**: incremental, exactly-once processing semantics using Structured Streaming under the hood.
# MAGIC - **Pipeline settings JSON**: pipelines can also be defined/deployed as JSON/YAML config (great for CI/CD via Databricks Asset Bundles).
# MAGIC - **Event log**: every pipeline run writes detailed lineage/quality/performance metrics to an event log table you can query with SQL.
# MAGIC
# MAGIC ### 11.4 DLT vs Traditional Spark Jobs — Interview Comparison
# MAGIC
# MAGIC | Traditional Spark/Notebook Job | DLT / Lakeflow Pipeline |
# MAGIC |---|---|
# MAGIC | You manually write orchestration (`dbutils.notebook.run`, job dependency graphs) | Dependency graph is **auto-inferred** from SQL/Python table references |
# MAGIC | You manually manage checkpoints for streaming | Checkpoints and state management handled automatically |
# MAGIC | Data quality checks = manual `assert`/custom code | Declarative `CONSTRAINT ... EXPECT` clauses |
# MAGIC | CDC = hand-written `MERGE` statements | `AUTO CDC` API |
# MAGIC | Monitoring = scattered logs | Unified pipeline DAG UI + event log table |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 12. Query Profiling
# MAGIC
# MAGIC **Query Profile** is Databricks SQL's built-in **query execution plan visualizer** — it shows exactly how a query ran, stage by stage, so you can diagnose performance problems.
# MAGIC
# MAGIC ### 12.1 How to access it
# MAGIC - Run a query in the SQL Editor → click **"Query Profile"** (or find it via **Query History**) → opens a **visual DAG** of the query's execution stages.
# MAGIC
# MAGIC ### 12.2 What it shows
# MAGIC - **Execution plan graph**: each node = an operator (scan, join, aggregate, shuffle, sort, etc.), with **time spent** and **rows processed** per node.
# MAGIC - **Bottleneck highlighting**: Databricks automatically highlights the **slowest stage** so you know where to focus.
# MAGIC - **Key metrics to check:**
# MAGIC   - **Bytes spilled to disk** → indicates the warehouse/cluster is **too small** for the workload (not enough memory) → action: increase cluster size.
# MAGIC   - **Rows read vs. rows returned** → huge disparity may indicate a missing filter pushdown or the need for better clustering/partitioning.
# MAGIC   - **Task skew** → some tasks take far longer than others → indicates **data skew** (uneven key distribution in a join/group-by).
# MAGIC   - **I/O time vs. compute time** → helps decide if the bottleneck is storage/network or CPU.
# MAGIC   - **Photon usage indicator** → confirms whether Photon accelerated the query or fell back to the regular Spark engine (some operators/UDFs aren't Photon-compatible).
# MAGIC
# MAGIC ### 12.3 How Query Profile connects to Warehouse Sizing
# MAGIC Per Databricks' own guidance (see Section 3): if the Query Profile shows **spill to disk**, that's the definitive signal to **bump up cluster size** — a classic exam question link between "Query Profiling" and "Warehouse Sizing" topics.
# MAGIC
# MAGIC ### 12.4 Typical interview answer
# MAGIC > "Query Profile is used to debug slow queries by visually breaking down each stage of the Spark execution plan, showing metrics like spill, skew, and rows scanned, so I can decide whether to rewrite the query, add clustering keys, or resize the warehouse."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 13. Query Caching
# MAGIC
# MAGIC Databricks SQL has **three distinct caching layers**. Interviewers love asking "what are the types of caching in Databricks SQL?" — know all three by name.
# MAGIC
# MAGIC ### 13.1 The Three Cache Types
# MAGIC
# MAGIC | Cache Type | Scope | Lifetime | Applies To |
# MAGIC |---|---|---|---|
# MAGIC | **Databricks SQL UI Cache** | Per-user | Up to **7 days** | SQL Editor & legacy dashboards (not AI/BI dashboards, which have their own caching) |
# MAGIC | **Query Result Cache** (Local + Remote) | Warehouse / workspace-wide | Until data changes or cache evicted | Any client (SQL editor, BI tools, JDBC/ODBC) |
# MAGIC | **Disk Cache** (formerly "Delta Cache") | Per-cluster/warehouse compute nodes | While cluster is running | Speeds up **repeated scans of the same underlying files** |
# MAGIC
# MAGIC ### 13.2 Databricks SQL UI Cache
# MAGIC - Shows the **most recent result** immediately when you reopen a query/dashboard — including results from scheduled runs — without re-executing anything.
# MAGIC - Max lifecycle: **7 days**, stored in your Databricks account's internal filesystem.
# MAGIC - Re-running the query **evicts** the old cached result and replaces it.
# MAGIC
# MAGIC ### 13.3 Query Result Cache (Local & Remote)
# MAGIC - Once a query executes, its **result set is cached**. Running the **exact same query text** again (even with different casing/whitespace, per real-world reports) returns the cached result **instantly without recomputation**.
# MAGIC - The **Remote Result Cache** is only available on **Serverless SQL Warehouses** — meaning the cache **persists even if the warehouse is stopped and restarted**, and can even be shared **across different serverless warehouses**. This does **not** work on Classic/Pro warehouses.
# MAGIC - **Disable caching** for benchmarking with:
# MAGIC   ```sql
# MAGIC   SET use_cached_result = false;
# MAGIC   ```
# MAGIC - **Cache invalidation** happens automatically if underlying data changes, or if the query uses **non-deterministic functions** like `current_timestamp()`, `rand()`, or dynamic views — these always force fresh execution.
# MAGIC - Users **cannot manually list or delete** individual cached entries — it's fully managed.
# MAGIC
# MAGIC ### 13.4 Disk Cache (formerly Delta Cache)
# MAGIC - Caches **raw file data** (Parquet/Delta files) read from cloud storage onto **local SSD of the compute nodes**, so subsequent scans of the same files are much faster (avoids re-fetching from cloud object storage).
# MAGIC - Automatically managed; benefits queries that repeatedly scan the same hot tables.
# MAGIC
# MAGIC ### 13.5 AI/BI Dashboards — separate caching behavior
# MAGIC - AI/BI dashboards maintain their own **24-hour, best-effort result cache** to speed up initial dashboard loads.
# MAGIC - Governed by **"dataset performance thresholds"** — small vs. large datasets are optimized differently.
# MAGIC
# MAGIC ### 13.6 Interview Summary Answer
# MAGIC > "Databricks SQL has three caching layers: the **UI cache** (7-day, per-user, for instant reload of query/dashboard results), the **query result cache** (identical-query reuse, with a remote variant unique to serverless warehouses that survives restarts), and the **disk cache** (local SSD caching of raw table files to speed up repeated scans). Non-deterministic functions bypass caching entirely."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 14. Query Scheduling
# MAGIC
# MAGIC **Query Scheduling** lets you run a saved SQL query **automatically on a recurring basis** — without a user manually clicking "Run."
# MAGIC
# MAGIC ### 14.1 How to schedule a query
# MAGIC 1. Open a saved query in the SQL Editor.
# MAGIC 2. Click **Schedule** (or **Add Schedule**).
# MAGIC 3. Configure:
# MAGIC    - **Frequency** (every N minutes/hours/days/weeks/months)
# MAGIC    - **Starting time & time zone**
# MAGIC    - Optional: check **"Show cron syntax"** to define the schedule using **Quartz Cron Syntax** for fine-grained control.
# MAGIC    - **Warehouse override**: by default the same warehouse used for ad hoc runs powers the schedule too, but you can pick a **different (often cheaper/smaller) warehouse** just for the scheduled run.
# MAGIC    - **Destinations**: optionally notify users/Slack/email/webhooks when the scheduled run completes (mainly relevant when paired with Alerts).
# MAGIC
# MAGIC ### 14.2 Why schedule queries?
# MAGIC - Keep a **dashboard fresh** automatically (e.g., refresh every 15 minutes for near-real-time reporting).
# MAGIC - Feed **downstream consumers** (e.g., materialize a result set that another system polls).
# MAGIC - Combine with **Alerts** to periodically check business conditions (see Section 15).
# MAGIC
# MAGIC ### 14.3 Query Schedule vs Alert Schedule (important distinction!)
# MAGIC > A schedule on the **underlying query** and a schedule on an **Alert** built from that query are **independent** — running the alert's schedule does **not** require or reuse the query's own separate schedule, and vice versa. This trips people up in interviews and real usage.
# MAGIC
# MAGIC ### 14.4 Query Scheduling vs Pipeline Scheduling vs Job Scheduling
# MAGIC
# MAGIC | | Query Schedule | Pipeline Schedule | Job Schedule |
# MAGIC |---|---|---|---|
# MAGIC | What it re-runs | A single saved SQL query | An entire ETL pipeline (DAG of tables) | A Job (can chain notebooks, pipelines, SQL tasks, alerts, etc.) |
# MAGIC | Typical use | Refresh a dashboard dataset | Regularly ingest & transform data | Full production orchestration |
# MAGIC | Granularity | Simple frequency + cron | Frequency + continuous mode | Complex multi-task DAGs, retries, dependencies |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 15. Databricks SQL Alerts
# MAGIC
# MAGIC **Alerts** run a query on a schedule and **notify you** when a condition you define is met against the result — essentially "if this SQL query's result crosses a threshold, tell someone."
# MAGIC
# MAGIC ### 15.1 Core Concept
# MAGIC - You define: a **query**, a **condition** (e.g., `value > 1000` or a boolean column), and a **schedule**.
# MAGIC - Alert evaluates to one of: **`OK`**, **`TRIGGERED`**, or **`ERROR`** (in the modern alert system; the older **legacy alerts** system additionally had an `UNKNOWN` status which has been removed).
# MAGIC - Notifications go to configured **Destinations**: email, Slack, Teams, PagerDuty, generic webhooks, or specific users.
# MAGIC
# MAGIC ### 15.2 Modern Alerts vs Legacy Alerts (interview distinction)
# MAGIC
# MAGIC | | Legacy Alerts | Modern (current) Alerts |
# MAGIC |---|---|---|
# MAGIC | Query reuse | Alert references an **existing saved query** | Each alert **owns its own query definition**, authored directly in the alert editor — cannot reuse an existing saved query |
# MAGIC | Status values | `OK`, `TRIGGERED`, `UNKNOWN` | `OK`, `TRIGGERED`, `ERROR` (no `UNKNOWN`) |
# MAGIC | UI | Separate query + alert screens | Unified single editor |
# MAGIC
# MAGIC ### 15.3 Creating an Alert (steps)
# MAGIC 1. **Workspace → Create → Alert** (or **Alerts → + New Alert**).
# MAGIC 2. Write/select the query.
# MAGIC 3. Set the **Trigger condition** (e.g., column value, aggregation threshold).
# MAGIC 4. Click **Add Schedule** → configure frequency/cron + optionally a dedicated warehouse for running the alert.
# MAGIC 5. Configure **Destinations** (who gets notified) — if skipped, **you won't be notified** even when triggered.
# MAGIC 6. Save.
# MAGIC
# MAGIC ### 15.4 Common real-world alert patterns (great to mention in interviews)
# MAGIC - **Business KPI monitoring**: e.g., alert if daily revenue drops below a threshold.
# MAGIC - **Data quality monitoring**: alert if null-count or row-count anomalies appear in a critical table.
# MAGIC - **Cost/performance monitoring**: alert on **warehouse events** or **query history** system tables to catch slow queries or capacity issues.
# MAGIC - **Security/audit monitoring**: alert on unusual activity found in audit logs.
# MAGIC - **Monitoring Unity Catalog Metric Views**: alerts can reference a governed **metric view** by its fully qualified name.
# MAGIC
# MAGIC ### 15.5 Alerts as a Job Task (production pattern)
# MAGIC You can add a **"SQL Alert" task** inside a **Lakeflow Job**:
# MAGIC - Runs the alert's condition check as part of a pipeline trigger.
# MAGIC - Downstream tasks can **branch based on the alert result** (e.g., stop the pipeline if a data-quality alert fires).
# MAGIC - Note: this evaluation is **independent** of the alert's own internal schedule — running it as a job task doesn't affect (or get affected by) the alert's standalone schedule.
# MAGIC - Requires a **Serverless or Pro** warehouse (Classic not supported for this task type).
# MAGIC - SQL alert tasks **do not support parameters**.
# MAGIC
# MAGIC ### 15.6 Refresh vs Schedule (subtle but testable)
# MAGIC - Clicking **"Refresh"** manually runs the query and updates the displayed status, but **does NOT send a notification** even if the state changes to `TRIGGERED`.
# MAGIC - **Notifications are only sent when the alert runs via its actual schedule** (or as a job task) — never on manual refresh.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 16. SQL Query vs SQL File
# MAGIC
# MAGIC A very practical, commonly asked "what's the difference" interview question.
# MAGIC
# MAGIC ### 16.1 SQL Query (in the SQL Editor / "Query" object)
# MAGIC - A **first-class Databricks object** (like a notebook or dashboard) stored in the workspace.
# MAGIC - Opens in the **native SQL Editor** experience.
# MAGIC - Natively supports: **query parameters, snippets, scheduling, alerts**, one-click visualization/charting, and is the object type dashboards reference as a "dataset."
# MAGIC - **Shows results of the single/last query cleanly** in a dedicated results pane with built-in visualization tools.
# MAGIC - Tightly integrated with **SQL Warehouses** — designed to run there (though notebooks can also attach to SQL Warehouses).
# MAGIC
# MAGIC ### 16.2 SQL File (a `.sql` file, typically in a Git-backed workspace folder / Repo)
# MAGIC - A **plain source-code file** (like a `.py` or `.sql` script) — great for **version control (Git)** and CI/CD via **Databricks Asset Bundles (DABs)**.
# MAGIC - Can contain **multiple SQL statements** — often used as the **source code for a Lakeflow Pipeline's transformations folder** (this is exactly what you saw in the CREATE STREAMING TABLE topic — pipelines read `.sql` files as their transformation logic).
# MAGIC - Behaves more like a **script**: run top-to-bottom, not an interactive single-query workbench.
# MAGIC - ⚠️ **Common real-world gotcha**: when a `.sql` query file is deployed via a Databricks Asset Bundle into another workspace, it can sometimes get misidentified and **open as a Notebook instead of a SQL Query object**, because the underlying representation (`.dbquery.ipynb` vs `.sql`) differs — a genuine, frequently-reported pain point.
# MAGIC
# MAGIC ### 16.3 SQL Query / SQL File vs. Notebook with SQL cells
# MAGIC
# MAGIC | | SQL Editor Query | SQL File | Notebook (`%sql` cells / SQL-default notebook) |
# MAGIC |---|---|---|---|
# MAGIC | Best for | Interactive ad hoc analysis, dashboards, alerts | Version-controlled pipeline source code, CI/CD | Exploratory + mixed-language work (SQL + Python + Markdown) |
# MAGIC | Multiple statements shown at once | Each run shows its own result (many queries visible) | N/A — it's just source text | Only the **last** cell's/statement's output shows per cell |
# MAGIC | Runs on | SQL Warehouse | SQL Warehouse (as pipeline source) or Compute cluster | All-purpose cluster (or attached SQL Warehouse for SQL cells) |
# MAGIC | Git/DevOps friendly | Less so (native object) | ✅ Yes — plain text file | ✅ Yes, `.ipynb`/`.py` source format |
# MAGIC | Supports parameters/snippets/alerts natively | ✅ | Partial (parameters exist inside pipeline context) | Limited (would use widgets instead) |
# MAGIC
# MAGIC ### 16.4 Interview one-liner
# MAGIC > "A SQL Query is a Databricks-native object built for interactive SQL Warehouse work — supporting parameters, snippets, scheduling, and alerts out of the box. A SQL File is a plain-text script, better suited for version control and being consumed as source code, e.g., inside a Git-backed Lakeflow Pipeline's transformations folder."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 17. Databricks ETL Jobs for Orchestration (Lakeflow Jobs)
# MAGIC
# MAGIC ### 17.1 What is a Databricks Job?
# MAGIC
# MAGIC A **Job** (branded as **Lakeflow Jobs**) is Databricks' **native orchestrator** — it lets you schedule and chain together **multiple tasks** into a single automated workflow (a DAG), similar in spirit to Airflow but built into the platform.
# MAGIC
# MAGIC ### 17.2 Task Types you can orchestrate in one Job
# MAGIC - **Notebook** task
# MAGIC - **Python script / Python wheel** task
# MAGIC - **SQL** task (run a saved query, a **dashboard refresh**, or a **legacy dashboard**)
# MAGIC - **Pipeline** task (trigger a Lakeflow Declarative Pipeline / DLT update)
# MAGIC - **SQL Alert** task (evaluate an alert; see Section 15.5)
# MAGIC - **dbt** task
# MAGIC - **JAR** task
# MAGIC - **Run Job** task (call another Job — enables modular, reusable workflows)
# MAGIC - **If/else condition**, **For each** loop task (for parameterized fan-out)
# MAGIC
# MAGIC ### 17.3 Why Jobs matter for ETL orchestration
# MAGIC - **Dependencies**: define `depends_on` relationships between tasks → build a true DAG (e.g., "run Silver transform only after Bronze ingestion succeeds").
# MAGIC - **Retries & timeouts**: configure automatic retry policies per task.
# MAGIC - **Scheduling**: cron-based or trigger-based (e.g., **file arrival trigger**, **table update trigger** for event-driven ETL).
# MAGIC - **Parameterization**: pass job-level parameters down into notebooks/SQL tasks/pipelines dynamically.
# MAGIC - **Compute flexibility**: each task can use a **different** cluster/warehouse — e.g., heavy Python transform on a job cluster, final reporting query on a SQL Warehouse.
# MAGIC - **Alerting/notifications**: configure email/Slack/webhook notifications on job start, success, or failure.
# MAGIC - **Observability**: full run history, Gantt-chart-style timeline view, and per-task logs.
# MAGIC
# MAGIC ### 17.4 Typical Production ETL Orchestration Pattern
# MAGIC
# MAGIC ```
# MAGIC [Lakeflow Pipeline Task: Bronze→Silver→Gold]
# MAGIC         ↓ (on success)
# MAGIC [SQL Alert Task: data quality check on Gold table]
# MAGIC         ↓ (if OK)
# MAGIC [SQL Task: refresh downstream AI/BI Dashboard]
# MAGIC         ↓
# MAGIC [Notification: Slack "ETL completed successfully"]
# MAGIC ```
# MAGIC
# MAGIC ### 17.5 Lakeflow Jobs vs Lakeflow Pipelines (a subtle but important distinction — commonly confused!)
# MAGIC
# MAGIC | | Lakeflow **Pipeline** | Lakeflow **Job** |
# MAGIC |---|---|---|
# MAGIC | Purpose | Declarative **data transformation** (bronze→silver→gold) | **Orchestration** — chaining any kind of task, including pipelines |
# MAGIC | Granularity | Table-level DAG (auto-inferred from SQL) | Task-level DAG (explicitly defined dependencies) |
# MAGIC | Contains | Streaming tables, materialized views, flows | Notebooks, SQL, pipelines, alerts, dbt, scripts, and even other jobs |
# MAGIC | Analogy | The "transformation engine" | The "conductor" that calls the engine plus everything else around it |
# MAGIC
# MAGIC > 🎯 **Interview one-liner**: "A Pipeline transforms data declaratively; a Job orchestrates — a Job typically *contains* a Pipeline task as one step among several (ingestion trigger → pipeline run → quality alert → dashboard refresh → notification)."
# MAGIC
# MAGIC ### 17.6 Deployment/CI-CD note
# MAGIC Production teams typically define Jobs and Pipelines as code using **Databricks Asset Bundles (DABs)** (YAML config), enabling Git-based CI/CD across dev/staging/prod workspaces — this ties back to the SQL File vs SQL Query distinction in Section 16.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 18. Rapid-Fire Interview Q&A Bank
# MAGIC
# MAGIC **Q1. What is a SQL Warehouse in Databricks?**
# MAGIC A compute resource purpose-built for running SQL queries/BI workloads on Lakehouse data, exposed via SQL Editor, JDBC/ODBC, dashboards, and BI tools. Formerly called a "SQL Endpoint."
# MAGIC
# MAGIC **Q2. Name the three (four) SQL warehouse types.**
# MAGIC Serverless, Pro, Classic, and the newer specialized Lakehouse Real-Time (Beta).
# MAGIC
# MAGIC **Q3. What is the single biggest differentiator of Serverless warehouses?**
# MAGIC **Intelligent Workload Management (IWM)** — ML-driven, rapid autoscaling with 2–6 second startup, vs. rule-based scaling and ~4-minute startup for Pro/Classic.
# MAGIC
# MAGIC **Q4. How does Classic/Pro autoscaling decide to add clusters?**
# MAGIC Based on estimated time to clear the queue: 2–6 min → +1 cluster, 6–12 min → +2, 12–22 min → +3, beyond that +3 plus +1 per extra 15 min. Also: 5 minutes stuck in queue forces a scale-up; 15 minutes of low load triggers scale-down.
# MAGIC
# MAGIC **Q5. What's the max queue size for any SQL warehouse?**
# MAGIC 1,000 queries.
# MAGIC
# MAGIC **Q6. What signal in the Query Profile tells you to increase warehouse size?**
# MAGIC "Bytes spilled to disk."
# MAGIC
# MAGIC **Q7. Name the three types of caching in Databricks SQL.**
# MAGIC UI Cache (7-day, per-user), Query Result Cache (Local + Remote; Remote only on Serverless), and Disk Cache (formerly Delta Cache, caches raw files on local SSD).
# MAGIC
# MAGIC **Q8. Does the query result cache work with `current_timestamp()`?**
# MAGIC No — non-deterministic functions always force re-execution and bypass the cache.
# MAGIC
# MAGIC **Q9. What's the difference between SCD1 and SCD2?**
# MAGIC SCD1 overwrites (no history); SCD2 keeps every version with validity windows (`__START_AT`/`__END_AT`).
# MAGIC
# MAGIC **Q10. What replaced the `APPLY CHANGES` API?**
# MAGIC `AUTO CDC` (same syntax, new name; `APPLY CHANGES` still works but is legacy).
# MAGIC
# MAGIC **Q11. What's the difference between `AUTO CDC` and `AUTO CDC FROM SNAPSHOT`?**
# MAGIC `AUTO CDC` consumes an existing CDC feed (SQL + Python); `AUTO CDC FROM SNAPSHOT` infers changes by diffing successive snapshots (Python-only).
# MAGIC
# MAGIC **Q12. Can you use `SEQUENCE BY` with multiple columns?**
# MAGIC Yes — wrap them in `STRUCT(col1, col2)`.
# MAGIC
# MAGIC **Q13. What's a Streaming Table backed by?**
# MAGIC A pipeline (Lakeflow Declarative Pipeline).
# MAGIC
# MAGIC **Q14. Can you `ALTER TABLE` a streaming table directly?**
# MAGIC No — must use `CREATE OR REFRESH` or `ALTER STREAMING TABLE`.
# MAGIC
# MAGIC **Q15. Difference between Materialized View and Streaming Table in a pipeline?**
# MAGIC Streaming table = incremental append-only processing of new data; Materialized View = precomputed, refreshed result of a (often aggregating/joining) query, recomputed on refresh.
# MAGIC
# MAGIC **Q16. What does `EXPECT ... ON VIOLATION FAIL UPDATE` do?**
# MAGIC Fails the entire pipeline update if any row violates the data-quality constraint (vs. `DROP ROW` which just discards bad rows).
# MAGIC
# MAGIC **Q17. What's the difference between a Lakeflow Pipeline and a Lakeflow Job?**
# MAGIC Pipeline = declarative data transformation DAG; Job = general-purpose orchestrator that can include pipeline runs, notebooks, SQL tasks, alerts, dbt, etc.
# MAGIC
# MAGIC **Q18. Are Query Parameters and Query Snippets the same thing?**
# MAGIC No. Parameters make **one query** dynamic (user-editable values via widgets). Snippets are **reusable chunks of SQL text** you insert into any query.
# MAGIC
# MAGIC **Q19. Does refreshing an Alert manually send a notification?**
# MAGIC No — notifications only fire on the alert's actual **schedule** (or when run as a Job task), never on manual refresh.
# MAGIC
# MAGIC **Q20. What warehouse types are required for a SQL Alert Job task?**
# MAGIC Serverless or Pro only — Classic is not supported.
# MAGIC
# MAGIC **Q21. What's the difference between a SQL Query object and a SQL File?**
# MAGIC Query = native workspace object with parameters/snippets/scheduling/alerts built in, ideal for interactive/BI work. File = plain-text `.sql` script, ideal for version control/CI-CD and as pipeline transformation source code.
# MAGIC
# MAGIC **Q22. Why is a serverless warehouse's Remote Result Cache special?**
# MAGIC It persists **across warehouse restarts** and can even be reused when switching to a **different** serverless warehouse — not possible on Classic/Pro.
# MAGIC
# MAGIC **Q23. What does Liquid Clustering (`CLUSTER BY`) replace?**
# MAGIC Traditional Hive-style static partitioning (`PARTITIONED BY`) — they're mutually exclusive; `CLUSTER BY AUTO` even lets Databricks pick the clustering keys automatically.
# MAGIC
# MAGIC **Q24. What's a "private" streaming table?**
# MAGIC A streaming table not registered in the catalog, visible only within its defining pipeline, and only persisted for the pipeline's lifetime (previously created with `TEMPORARY`).
# MAGIC
# MAGIC **Q25. What is Bitemporal AUTO CDC?**
# MAGIC A Beta feature extending SCD Type 2 to track two time dimensions simultaneously: business/valid time and system/transaction time.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 19. Cheat Sheet Summary Tables
# MAGIC
# MAGIC ### Warehouse Types
# MAGIC | Type | Photon | Predictive I/O | IWM | Startup | Compute location |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Serverless | ✅ | ✅ | ✅ | 2–6 sec | Databricks account |
# MAGIC | Pro | ✅ | ✅ | ❌ | ~4 min | Your cloud account |
# MAGIC | Classic | ✅ | ❌ | ❌ | ~4 min | Your cloud account |
# MAGIC | Lakehouse//RT (Beta) | ✅ | ✅ | ✅ | Sub-second query latency | Databricks account |
# MAGIC
# MAGIC ### SCD & CDC
# MAGIC | Concept | One-liner |
# MAGIC |---|---|
# MAGIC | SCD1 | Overwrite, no history |
# MAGIC | SCD2 | New row per change, `__START_AT`/`__END_AT` |
# MAGIC | AUTO CDC | Declarative SCD1/2 from a CDC feed (SQL+Python) |
# MAGIC | AUTO CDC FROM SNAPSHOT | Declarative SCD1/2 by diffing snapshots (Python only) |
# MAGIC | Bitemporal AUTO CDC | SCD2 + two time dimensions (Beta) |
# MAGIC
# MAGIC ### Caching
# MAGIC | Layer | Scope | Lifetime |
# MAGIC |---|---|---|
# MAGIC | UI Cache | Per user | 7 days |
# MAGIC | Result Cache (Remote) | Serverless warehouses | Until invalidated; survives restarts |
# MAGIC | Disk Cache | Compute node local SSD | While cluster/warehouse runs |
# MAGIC
# MAGIC ### Orchestration
# MAGIC | Object | Role |
# MAGIC |---|---|
# MAGIC | Streaming Table / Materialized View | Data unit inside a pipeline |
# MAGIC | Pipeline | Declarative transformation DAG (bronze→silver→gold) |
# MAGIC | Job | General orchestrator (chains pipelines, SQL, alerts, notebooks) |
# MAGIC | Alert | Threshold-based notification on a query result |
# MAGIC | Query Schedule | Auto-refresh a saved query |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Final Tip for the Interview
# MAGIC If asked an open-ended "explain SQL warehousing end-to-end" question, structure your answer as:
# MAGIC 1. **What** a SQL Warehouse is and **why** it exists (compute for BI/SQL workloads).
# MAGIC 2. **Types** and when to choose which (serverless-first mindset).
# MAGIC 3. **Sizing/scaling/queuing** mechanics (shows you understand cost/performance tradeoffs).
# MAGIC 4. **Productivity features**: parameters, snippets, caching, profiling.
# MAGIC 5. **Data engineering side**: streaming tables, SCD1/2, AUTO CDC, pipelines/DLT.
# MAGIC 6. **Operationalization**: scheduling, alerts, and Jobs for orchestration.
# MAGIC
# MAGIC This narrative shows both **platform knowledge** and **real-world engineering judgment** — exactly what interviewers are screening for.
# MAGIC
# MAGIC **Good luck! 🚀**