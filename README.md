
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

