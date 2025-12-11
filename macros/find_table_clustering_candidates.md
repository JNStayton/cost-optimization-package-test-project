{% docs find_table_clustering_candidates %}

## **Macro:** find_table_clustering_candidates

This document provides a guide to using and understanding the `find_table_clustering_candidates` macro. This macro helps you identify *which tables* need attention in terms of clustering, and can be followed up with the `suggest_clustering_keys` macro to identify columns for each table identified.

It performs a system-wide scan of your Snowflake account to identify large tables that are suffering from poor performance or severe storage fragmentation.

There are optional filters you may pass in to limit the scope of this evaluation.

### **How to Run:**

**Default Run**

Scans all tables in Snowflake that are a part of your specific dbt project that are larger than 1TB (Snowflake's recommended minimum for clustering benefits):

`dbt run-operation find_table_clustering_candidates`

**Run for Smaller Tables (Testing)**

Bypass the check within the macro for table size. This is ideal for running against smaller test tables in your development environment. To test on tables larger than 1GB:

`dbt run-operation find_table_clustering_candidates --args '{ignore_table_size: true}'`

**Run for Non-dbt managed Tables**

To scan every table in the Snowflake account (including raw ingestion tables not modeled in dbt):

`dbt run-operation find_table_clustering_candidates --args '{dbt_project_only: false}'`

**Run for Specific Databases/Schemas**

`dbt run-operation find_table_clustering_candidates --args '{target_schemas: ["SCH_1", "SCH_2"], target_databases: ["DB_1", "DB_2]}'`


### **1. What it Does & How it Works**

This macro acts as a diagnostic scanner. It combines storage metrics with query history to calculate a "Score" for every large table, as it pertains to clustering.

#### Step 1: Size & Scope Filtering

The macro first queries `SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS`. 
- **Size Check:** By default, it filters for tables > 1TB. You can override this to 1GB using the `ignore_table_size` argument.
- **Scope:** It identifies Permanent Tables, Transient Tables, and Materialized Views. These are all ideal candidates for table clustering. We exclude external tables, hybrid tables, dynamic tables, iceberg tables, event tables, and views.
- **dbt Mapping:** It compares the physical tables found in Snowflake against your dbt graph to map them to specific models in your project.

#### Step 2: Fragmentation Analysis (The Health Check)

This is the structural check. The macro calculates the ratio of **Micro-partitions to Total Rows** and weighs this score.

**Why it matters:** In a healthy Snowflake table, a single micro-partition should hold between 50MB and 500MB of data (and ideally hundreds of thousands of rows). 
- If your table has 100 rows per partition, your data is severely fragmented (over-partitioned). 
- This leads to high storage costs and very slow queries because Snowflake has to scan too much metadata.

#### Step 3: Usage Analysis

The macro queries `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` (defaulting to the last 7 days) to measure:
- **Query Volume:** How often is this table SELECTed from?
- **Churn:** How often is this table INSERTed/UPDATEd? (High churn makes clustering expensive).
- **Pruning Efficiency:** What percentage of the table is scanned on average?

#### The Output

The macro calculates a score based on usage volume multiplied by how "unhealthy" the table structure is. 

In preview_only mode, tt prints the top 10 candidates to your log:

> --- Top 10 Clustering Candidates ---
> ------------------------------------------------
> Table: DB.SCHEMA.TABLE
> dbt Model: model.project.model_name
> Table Type: Transient Table
> Score: 3560 | Potential candidate? True
> Table Size: 28 GB 
> Total rows: 600,000,000
> Current Micropartitions: 62,000,000
> Average Rows per Micropartition: 9.6
> Average Partitions Scanned: 813
> Usage: 116 SELECTs | 12 DMLs (Ratio: 8.9)
> Average Query Duration: 2.24s

In production mode (preview_only: false), the results are output into a model, built and queryable in Snowflake.

### **2. How to Interpret the Results**

The output gives you a physical profile of the table. Here are the key metrics to watch:

**1. The Score**
The score is a combination of how frequently the table is queried, how frequently the table is updated, and how fragmented the table is organizationally. A high score indicates a table that is both frequently queried and poorly optimized.

**2. Average Rows per Micropartition**
- A low number here indicates that your micropartitions are not of ideal size. This table is likely ingested in very small batches (e.g., row-by-row inserts). It could benefit from clustering or a change in ingestion strategy.

**3. Usage Ratio (SELECTs vs DMLs)**
- Clustering costs credits every time data is changed (DML). 
- Ideal Candidates have a **High Ratio**, meaning they are read much more often than they are written to.
- If the ratio is low (< 1.0), clustering will be expensive to maintain.

**4. Average Partitions Scanned**
- If this number is close to "Current Micropartitions", your queries are, on average, performing Full Table Scans. This table might benefit from a clustering key to help Snowflake prune unnecessary data.
- Keep in mind this is an average -- you might have a number of queries that are putting off your score. 

### **3. How to Customize the Macro**

**`lookback_days`** (Default: 7)
Increase this to 30 or 60 to get a more statistically significant view of query history.
`--args '{lookback_days: 30}'`

**`target_databases` / `target_schemas`**
Limit the scan to specific areas of your account to speed up execution or focus on a specific business unit.
`--args '{target_databases: ["ANALYTICS"], target_schemas: ["MART_SALES"]}'`

**`dbt_project_only`** (Default: true)
Set this to `false` to find raw tables or legacy tables that are not managed by dbt but are consuming credits. The default value will only give results for tables in Snowflake that correspond to models within the given dbt project.

### **4. Recommended Workflow**

1.  **Run the Scan:** Run `find_table_clustering_candidates` to identify your top 3 "problem" tables.
2.  **Analyze the Problem:**
    * If "Average Rows per Micropartition" is very low, the table could benefit from clustering.
    * If "Average Partitions Scanned" is high, the table needs a clustering key to improve filtering.
3.  **Find the Key:** Take the `dbt Model` name from the output and run the `suggest_clustering_keys` macro on it:
    `dbt run-operation suggest_clustering_keys --args '{model_name: stg_charges}'`
4.  **Test & Apply:** Follow the testing workflow defined in the `suggest_clustering_keys` documentation.

{% enddocs %}