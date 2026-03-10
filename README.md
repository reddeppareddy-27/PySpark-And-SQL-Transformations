
````markdown
# PySpark Data Engineering Practice  
## Basic Operations & SQL Transformations using Apache Spark

This repository contains hands-on practice notebooks focused on mastering **PySpark DataFrame operations** and **SQL-based transformations** using Apache Spark in a Databricks environment.

The goal of this project is to build strong foundational skills in data ingestion, schema management, and distributed data transformations.

---

## 📌 Project Overview

This project covers:

- Reading data from tables and CSV files
- Schema inference and explicit schema definition
- DataFrame transformations
- SQL vs PySpark transformation parity
- Writing processed data back to managed tables

Two notebooks are included:

1. **Basic Operations.ipynb**
2. **Pyspark And SQL Transformation.ipynb**

---

## 🏗 Technologies Used

- Apache Spark (PySpark)
- Spark SQL
- Databricks Environment
- Python

---

## 📥 Data Ingestion

Implemented multiple methods of loading structured data:

### 🔹 Reading from Managed Tables
```python
spark.read.table("table_name")
````

### 🔹 Reading CSV Files with Options

```python
spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", "|") \
    .load("path")
```

### 🔹 Handling Advanced CSV Scenarios

* Custom delimiters
* Multi-line records
* Quote and escape characters

### 🔹 Explicit Schema Definition

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
```

---

## 🔄 DataFrame Transformations

The notebooks demonstrate core PySpark transformations:

* `select()`
* `selectExpr()`
* `withColumn()`
* `drop()`
* `filter()` / `where()`
* `distinct()`
* `union()` and `unionAll()`
* `orderBy()`
* `cast()` (Data type conversion)

### Example:

```python
df.select("name", "age").filter("age > 25")
```

---

## 🧮 SQL vs PySpark Transformation Parity

Each transformation was implemented in:

* PySpark DataFrame API
* Equivalent Spark SQL queries

### Example – Derived Column (PySpark)

```python
df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
```

### Equivalent SQL

```sql
SELECT CONCAT(first_name, ' ', last_name) AS full_name
FROM table_name;
```

This helped in understanding how Spark executes transformations regardless of the interface used.

---

## 💾 Writing Data Back to Tables

Processed datasets were stored using:

```python
df.write.saveAsTable("processed_table")
```

This demonstrates working with managed schemas and persistent storage.

---

## 🎯 Key Learning Outcomes

* Strong understanding of schema management
* Practical experience with distributed data transformations
* Clear difference between UNION and UNION ALL
* Hands-on CSV ingestion challenges
* SQL to PySpark mapping
* Foundational Big Data processing concepts

---

## 📈 Why This Project Matters

Mastering DataFrame operations and SQL transformations is critical before designing large-scale ETL pipelines. This project focuses on building core data engineering fundamentals using Apache Spark.

---

## 🚀 Next Steps

* Joins and aggregations
* Window functions
* Performance optimization
* Partitioning strategies
* End-to-end ETL pipeline design

---

## 👨‍💻 Author

Undergraduate student focused on becoming a Data Engineer by building strong fundamentals in distributed data processing systems.

---

I analyzed the three notebooks you uploaded:

1. **Types Of Data.ipynb** – Working with PySpark data types and transformations
2. **Aggregators - PySpark & SQL.ipynb** – Aggregation operations and window functions
3. **Complex Types In PySpark.ipynb** – Arrays, Structs, and JSON handling

Below is a **clean, professional `README.md`** suitable for a **GitHub repository**.

---

# PySpark Data Transformations and Data Types

This repository contains hands-on examples demonstrating how to work with **different data types, aggregation operations, and complex data structures in PySpark** using both **PySpark DataFrame APIs and Spark SQL**.

The notebooks are designed to help learners understand how data transformations are performed in distributed data processing environments.

---

# Repository Structure

```
.
├── Types Of Data.ipynb
├── Aggregators - PySpark & SQL.ipynb
├── Complex Types In PySpark.ipynb
└── README.md
```

Each notebook focuses on a specific area of PySpark data processing.

---

# 1. Working with Different Data Types

Notebook: **Types Of Data.ipynb**

This notebook demonstrates how PySpark handles common data types and transformations while working with datasets.

### Topics Covered

* Boolean filtering operations
* Numeric functions and calculations
* String transformations
* Regular Expressions (Regex)
* Date and Timestamp functions
* Null value handling

### Key PySpark Functions

```
where()
lower()
upper()
trim()
substring()
split()
coalesce()
fill()
drop()
```

### SQL Equivalents

The notebook also demonstrates how the same operations can be implemented using **Spark SQL queries**.

Example:

```sql
SELECT *
FROM samples.bakehouse.sales_transactions
WHERE paymentMethod != 'mastercard'
```

This helps in understanding the differences between **DataFrame API and SQL-based transformations**.

---

# 2. Aggregations in PySpark and SQL

Notebook: **Aggregators - PySpark & SQL.ipynb**

This notebook focuses on performing **data aggregation and analytical computations** using PySpark.

### Basic Aggregations

* `count()`
* `min()`
* `max()`
* `sum()`
* `avg()`

### Grouped Aggregations

Using:

```
groupBy()
agg()
```

Example use cases include summarizing transactional data and computing metrics.

### Advanced Aggregations

The notebook also introduces **Window Functions**, which are widely used in analytical pipelines.

Functions covered include:

```
row_number()
rank()
dense_rank()
```

### Use Cases

* Ranking records
* Finding top values
* Deduplication logic
* Analytical computations

Both **PySpark implementations and Spark SQL equivalents** are demonstrated.

---

# 3. Working with Complex Data Types

Notebook: **Complex Types In PySpark.ipynb**

Modern data platforms often handle **semi-structured and nested data**. This notebook explores how PySpark processes complex data types.

### Complex Data Types Covered

* Arrays
* Structs
* JSON

### Array Operations

```
size()
array_contains()
split()
explode()
```

These functions allow efficient manipulation of array-based columns.

### Struct Operations

Nested fields can be accessed using **dot notation**.

Example:

```
info.age
info.city
```

### JSON Processing

The notebook demonstrates how to extract and convert JSON data using:

```
get_json_object()
from_json()
```

This allows JSON strings to be transformed into **structured columns** for easier querying and analysis.

---

# Technologies Used

* PySpark
* Spark SQL
* Databricks Notebook Environment

---

# Learning Objectives

Through these notebooks, you will learn how to:

* Perform data transformations using PySpark
* Apply aggregation operations on large datasets
* Use window functions for analytical queries
* Work with nested and semi-structured data
* Implement both **PySpark and SQL approaches** to data processing

---

# Use Case

These examples simulate real-world data engineering scenarios such as:

* Transaction data analysis
* Data transformation pipelines
* Processing nested and semi-structured datasets
* Analytical computations on distributed data systems

---


---

# Modern Data Engineering with Delta Lake, PySpark & DQX on Databricks

## Project Overview

This project demonstrates several **modern Data Engineering concepts using PySpark, Delta Lake, and Databricks**. It explores how scalable data pipelines are built using the **Lakehouse architecture**, including **data quality validation, incremental processing, and performance optimization**.

The project covers:

* Delta Lake table operations
* Change Data Feed (CDF)
* Data Quality Framework using DQX
* Medallion Architecture
* Partitioning and Liquid Clustering
* Delta Lake optimization techniques
* Data transformation with PySpark

These implementations simulate **real-world data engineering workflows used in modern cloud data platforms**.

---

# Architecture

The project follows the **Medallion Architecture** commonly used in Databricks.

Bronze → Silver → Gold

Bronze Layer
Raw data ingestion from source systems.

Silver Layer
Data cleansing, validation, and transformation.

Gold Layer
Curated datasets optimized for analytics and reporting.

Data Quality checks using **DQX** ensure reliable data as it moves through the pipeline.

---

# Technologies Used

* PySpark
* Delta Lake
* Databricks
* Databricks Labs DQX
* SQL
* YAML
* Medallion Architecture

---

# Key Concepts Implemented

## Delta Lake Features

* ACID transactions
* Schema evolution
* Time travel
* Table restore
* Merge (upsert) operations
* Optimize and Z-Ordering
* Vacuum cleanup

---

## Change Data Feed (CDF)

Change Data Feed tracks **row-level changes** in Delta tables.

It captures:

* INSERT
* UPDATE
* DELETE

This enables **incremental pipelines instead of full table processing**.

---

## Data Quality Framework (DQX)

The Databricks Labs **DQX framework** was used to implement automated data validation.

Capabilities include:

* Data profiling
* Rule generation
* Row-level validation
* Column-level validation
* Quarantine of bad records

---

## Handling Bad Data

Two approaches were explored.

### Quarantine

Invalid records are separated for investigation.

### Flagging

Records remain in the dataset but are marked with quality indicators.

---

## Delta Lake Optimization

Performance optimizations implemented:

* Partitioning
* Liquid clustering
* Optimize
* Z-Ordering
* Vacuum

These techniques improve query performance and storage efficiency.

---

# Repository Structure

```
.
├── notebooks
│   ├── Change_Data_Feed.ipynb
│   ├── Data_Quality_Frameworks.ipynb
│   ├── Liquid_Clustering_Partitioning.ipynb
│   ├── Regex_Text_Processing.ipynb
│   └── Delta_Operations.ipynb
│
├── README.md
```

---

# Implementation

---

# 1 Change Data Feed Implementation

### Create Delta Table with Change Data Feed

```sql
CREATE TABLE my_files.my_schema.change_feed_data(
first_name string,
age long
)
using delta
TBLPROPERTIES(delta.enableChangeDataFeed=true)
```

### Insert Data

```python
df=spark.createDataFrame([("bob",23),("sue",98),("jim",27)]).toDF("first_name","age")

df.write.mode("append").format("delta").saveAsTable("my_files.my_schema.change_feed_data")
```

### View Data

```sql
select * from my_files.my_schema.change_feed_data
```

### Delete Record

```sql
delete from my_files.my_schema.change_feed_data
where first_name='bob'
```

### Update Record

```sql
update my_files.my_schema.change_feed_data
set age=35
where first_name='sue'
```

### Read Change Data Feed

```python
df=spark.read.format("delta")\
.option("readChangeFeed","true")\
.option("startingVersion",0)\
.table('my_files.my_schema.change_feed_data')

display(df)
```

---

# 2 Data Quality Framework using DQX

## Install DQX

```python
%pip install databricks-labs-dqx
%restart_python
```

---

## Create Sensor Data Table

```sql
CREATE TABLE IF NOT EXISTS dqx_demo.dqx.sensor_data (
sensor_id STRING,
machine_id STRING,
sensor_type STRING,
reading_value DOUBLE,
reading_timestamp TIMESTAMP,
calibration_date DATE,
battery_level INT,
facility_zone STRING,
is_active BOOLEAN,
firmware_version STRING,
ingest_date DATE
)
USING DELTA
```

---

## Load Sensor Data

```python
sensor_bronze_data=spark.read.table('dqx_demo.dqx.sensor_data')
display(sensor_bronze_data.limit(10))
```

---

## Data Profiling

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler

ws=WorkspaceClient()
profiler=DQProfiler(ws)

summary_stats,profiles=profiler.profile(sensor_bronze_data)
display(summary_stats)
```

---

## Generate Data Quality Rules

```python
from databricks.labs.dqx.profiler.generator import DQGenerator

generator=DQGenerator(ws)
maintenance_checks=generator.generate_dq_rules(profiles)
```

---

## Apply Data Quality Checks

```python
valid_df,quarantined_df=dq_engine.apply_checks_by_metadata_and_split(
sensor_bronze_data,
maintenance_checks
)
```

---

# Example YAML Data Quality Rules

```yaml
- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
    - sensor_id
    - machine_id

- criticality: warn
  check:
    function: regex_match
    arguments:
      column: machine_id
      regex: '^MCH-\d{3}$'
```

---

# 3 Partitioning

Partitioning organizes data into folders for faster query performance.

### Create Partitioned Table

```sql
CREATE OR REPLACE TABLE my_files.my_schema.part_table1(
order_id int,
order_date date,
country STRING,
category string,
amount double
)
USING DELTA
PARTITIONED BY (country,category)
```

---

# 4 Liquid Clustering

Clustering organizes data within files based on selected columns.

```sql
CREATE OR REPLACE TABLE my_files.my_schema.cluster_table(
order_id int,
order_date date,
country STRING,
category string,
amount double
)
USING DELTA
CLUSTER BY (country,category,order_date)
```

---

# 5 PySpark Text Processing

Example dataset:

```python
data=[
("OrderID :564974",),
("Invoice # 9821 Processed",),
("No numbers here",),
("ref-44965,",)
]

df=spark.createDataFrame(data,["text_col"])
display(df)
```

This demonstrates **extracting structured values from semi-structured text data**.

---

# 6 Delta Lake Operations

### Create Delta Table

```python
data=[Row(id=1,name='Alice',age=30),
Row(id=2,name='Bob',age=20),
Row(id=3,name='Charlie',age=40)]

df=spark.createDataFrame(data)

df.write.format('delta').mode('overwrite').saveAsTable('my_files.my_schema.people_delta_demo')
```

---

## Schema Evolution

```python
df.write.format('delta').mode('append')\
.option('mergeSchema','true')\
.saveAsTable('my_files.my_schema.people_delta_demo')
```

---

## Time Travel

```sql
select * from my_files.my_schema.people_delta_demo
version as of 2
```

---

## Restore Table

```sql
restore my_files.my_schema.people_delta_demo
version as of 2
```

---

## Merge (Upsert)

```sql
MERGE INTO my_files.my_schema.people_delta_demo AS target
USING update as source
ON target.id=source.id

WHEN MATCHED THEN
UPDATE SET target.name=source.name,target.age=source.age

WHEN NOT MATCHED THEN
INSERT (id,name,age)
values(source.id,source.name,source.age)
```

---

# Optimization

### Optimize

```sql
optimize my_files.my_schema.people_delta_demo
```

---

### Z-Order

```sql
optimize my_files.my_schema.people_delta_demo
zorder by id
```

---

### Vacuum

```sql
vacuum my_files.my_schema.people_delta_demo
```

---

# Key Learnings

Through this project, the following data engineering concepts were implemented:

* Delta Lake table management
* Incremental processing with Change Data Feed
* Data quality validation using DQX
* Data pipeline architecture using Medallion layers
* Query performance optimization techniques
* Data transformation using PySpark

---

# Future Improvements

Possible enhancements include:

* Streaming pipelines using Structured Streaming
* Integration with Kafka
* Automated orchestration using Airflow
* Real-time data quality monitoring

---

# Author

MASIREDDY REDDEPPA REDDY

Aspiring Data Engineer focused on building scalable data pipelines using PySpark, Delta Lake, and modern Lakehouse architectures.

---

If you want, I can also give you **a much stronger GitHub README (top 1% level)** with:

* **architecture diagram**
* **pipeline flow diagram**
* **project screenshots**
* **resume-ready project description**

That version can make your **GitHub look like a real Data Engineer portfolio project**.
