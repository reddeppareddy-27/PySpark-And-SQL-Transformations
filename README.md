
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
