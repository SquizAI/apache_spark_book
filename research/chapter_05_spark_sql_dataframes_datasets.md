# Chapter 5: Spark SQL, DataFrames, and Datasets

## Topic

This chapter transitions from the low-level RDD API to Spark SQL, Spark's powerful module for working with structured and semi-structured data. We will explore DataFrames and Datasets, which are higher-level abstractions that provide a more optimized and user-friendly way to perform complex data analysis. We'll cover how to create them from various sources, perform common operations using both their programmatic APIs and SQL queries, manage schemas, and leverage User-Defined Functions. Finally, we'll briefly introduce the advanced optimization techniques within Spark SQL, namely the Catalyst Optimizer and Tungsten execution engine.

## Keywords

Spark SQL, DataFrame API, Dataset API, Structured Data, Semi-Structured Data, Schema, `SparkSession`, `DataFrameReader`, `DataFrameWriter`, JSON, Parquet, CSV, Avro, ORC, JDBC, Hive Tables, `select()`, `filter()`, `where()`, `groupBy()`, `agg()`, `join()`, `orderBy()`, `withColumn()`, `col()`, SQL Queries, `spark.sql()`, Encoders, Type Safety, RDD to DataFrame, DataFrame to RDD, Data Sources API, User-Defined Functions (UDF), User-Defined Aggregate Functions (UDAF), Catalyst Optimizer, Tungsten Execution Engine, Spark Optimizations.

## Structure

1.  [Introduction to Spark SQL](#introduction-to-spark-sql)
    *   What is Spark SQL?
    *   Advantages over RDDs for Structured Data
    *   Unified Data Access
    *   The `SparkSession`: Entry Point for Spark SQL
2.  [DataFrames: Structured Data Processing](#dataframes-structured-data-processing)
    *   What is a DataFrame?
        *   Distributed Collection of Data Organized into Named Columns
        *   Analogy to Relational Tables or Python/R DataFrames
    *   Creating DataFrames
        *   From RDDs (e.g., `rdd.toDF()`, `spark.createDataFrame(rdd, schema)`)
        *   From Files: JSON, Parquet, CSV, Text, ORC, Avro
        *   From Hive Tables
        *   From Relational Databases via JDBC
        *   From Existing Collections (Scala/Python lists)
    *   Schema Management
        *   Schema Inference
        *   Programmatically Defining a Schema (`StructType`, `StructField`)
        *   `printSchema()`
    *   Common DataFrame Operations (Transformations)
        *   `select(cols: Column*)`, `selectExpr(exprs: String*)`
        *   `filter(condition: Column)`, `where(condition: String)`
        *   `groupBy(cols: Column*)`, `agg(exprs: Map[String, String])` or `agg(expr: Column, exprs: Column*)`
        *   `join(right: Dataset[_], joinExprs: Column, joinType: String)`
        *   `orderBy(sortExprs: Column*)`, `sort(sortExprs: Column*)`
        *   `withColumn(colName: String, col: Column)`, `withColumnRenamed(existingName: String, newName: String)`
        *   `drop(colNames: String*)`
        *   `distinct()`, `dropDuplicates()`
        *   `limit(n: Int)`
        *   `union(other: Dataset[T])`, `unionByName(other: Dataset[T])`
        *   Using Column Expressions and Functions (`col()`, `lit()`, string functions, date functions, math functions, etc.)
    *   DataFrame Actions
        *   `show(numRows: Int, truncate: Boolean)`
        *   `collect()`
        *   `count()`
        *   `first()`, `head()`, `take(n)`
        *   `describe(cols: String*)`
        *   `write()` (covered in Data Sources API)
    *   Code Examples (Scala, Python)
3.  [Datasets: Type-Safe Structured Data Processing (Scala/Java)](#datasets-type-safe-structured-data-processing-scalajava)
    *   What is a Dataset?
        *   Typed Extension of DataFrame (`Dataset[T]`)
        *   Combination of RDD's type safety and DataFrame's optimization
    *   Benefits of Datasets
        *   Compile-time Type Safety
        *   Optimized Execution using Encoders
    *   Creating Datasets
        *   From RDDs of case classes (Scala) or Java beans (Java)
        *   From Existing Collections
        *   By applying typed transformations to DataFrames (`.as[T]`)
    *   Encoders
        *   Role in Serializing/Deserializing JVM objects for Tungsten
    *   Operations on Datasets (similar to DataFrames, but often with typed lambda functions)
    *   Code Examples (Scala, Java)
4.  [Running SQL Queries Programmatically](#running-sql-queries-programmatically)
    *   Using `spark.sql("SQL Query")`
    *   Creating Temporary Views (`createOrReplaceTempView("viewName")`)
    *   Creating Global Temporary Views (`createOrReplaceGlobalTempView("viewName")`)
    *   Mixing SQL with DataFrame/Dataset Operations
    *   Code Examples (Scala, Python, Java)
5.  [Interoperating with RDDs](#interoperating-with-rdds)
    *   Converting DataFrames/Datasets to RDDs (`.rdd`)
    *   Converting RDDs to DataFrames/Datasets
        *   Using `toDF("colName1", ...)` (Scala/Python)
        *   Using `spark.createDataFrame(rowRDD, schema)`
        *   Inferring Schema from Case Classes (Scala) or JavaBeans (Java)
    *   When to Convert?
6.  [Data Sources API: Loading and Saving Data](#data-sources-api-loading-and-saving-data)
    *   Generic `load()` and `save()` Methods
        *   `spark.read.format("...").option("key", "value").load("path")`
        *   `df.write.format("...").option("key", "value").save("path")`
    *   Specific Format Methods
        *   Parquet: `spark.read.parquet("...")`, `df.write.parquet("...")` (Default format)
        *   JSON: `spark.read.json("...")`, `df.write.json("...")`
        *   CSV: `spark.read.csv("...")`, `df.write.csv("...")` (Options: header, inferSchema, delimiter, etc.)
        *   JDBC: `spark.read.jdbc("url", "table", properties)`, `df.write.jdbc("url", "table", properties)`
        *   Text Files: `spark.read.text("...")`, `df.write.text("...")`
        *   Hive Tables: `spark.read.table("tableName")`, `df.write.saveAsTable("tableName")`
    *   Save Modes
        *   `SaveMode.ErrorIfExists` (Default)
        *   `SaveMode.Append`
        *   `SaveMode.Overwrite`
        *   `SaveMode.Ignore`
    *   Partitioning Data on Write (`partitionBy("colName", ...)`)
    *   Bucketing Data (`bucketBy(numBuckets, "colName", ...)`)
    *   Code Examples for various sources and sinks
7.  [User-Defined Functions (UDFs) and Aggregate Functions (UDAFs)](#user-defined-functions-udfs-and-aggregate-functions-udafs)
    *   User-Defined Functions (UDFs)
        *   Purpose: Extending Spark's built-in functions with custom logic.
        *   Creating and Registering UDFs (Scala: `udf()`, Python: `udf()`, Java: `udf()`)
        *   Using UDFs in DataFrame operations and SQL queries.
        *   Performance considerations (serialization, not optimized by Catalyst as well as built-in functions).
        *   Code Examples (Scala, Python)
    *   User-Defined Aggregate Functions (UDAFs)
        *   Purpose: Implementing custom aggregation logic beyond standard functions like `sum`, `avg`.
        *   More complex to implement (e.g., extending `UserDefinedAggregateFunction` in Scala/Java or `Aggregator` for typed UDAFs in Scala).
        *   Registering and using UDAFs.
        *   Code Examples (Scala)
8.  [Optimizations in Spark SQL: Catalyst and Tungsten (Overview)](#optimizations-in-spark-sql-catalyst-and-tungsten-overview)
    *   The Catalyst Optimizer
        *   Role: Extensible query optimizer.
        *   Phases: Analysis, Logical Optimization, Physical Planning, Code Generation.
        *   How it transforms DataFrame/Dataset operations and SQL queries into efficient execution plans.
    *   The Tungsten Execution Engine
        *   Role: Off-heap memory management, cache-aware computation, whole-stage code generation.
        *   How it improves CPU and memory efficiency.
    *   Impact on Performance: Why DataFrames/Datasets are generally faster than RDDs for structured data.
9.  [Chapter Summary](#chapter-summary)

---

## Introduction to Spark SQL

Spark SQL is a Spark module for structured data processing. It integrates relational processing with Spark's functional programming API. It provides more information about the structure of both the data and the computation being performed than the RDD API, allowing Spark to perform additional optimizations.

### Advantages over RDDs for Structured Data
*   **Optimization:** Spark SQL leverages the Catalyst optimizer to generate efficient query plans. It can understand the semantics of operations and the structure of data, leading to significant performance gains.
*   **Ease of Use:** The DataFrame and Dataset APIs, along with SQL query support, provide a more familiar and often more concise way to express complex data manipulations compared to RDD transformations.
*   **Uniform Data Access:** Spark SQL can read and write data from a wide variety of structured sources (e.g., JSON, Parquet, JDBC).

### The `SparkSession`: Entry Point for Spark SQL
As introduced in Chapter 2, `SparkSession` is the unified entry point for Spark functionality since Spark 2.0. It is used to create DataFrames, register DataFrames as tables, execute SQL over tables, cache tables, and read Parquet files.

```scala
// Scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder()
  .appName("Spark SQL Basics")
  .config("spark.master", "local") // Example configuration
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._
```

```python
# Python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Spark SQL Basics") \
    .config("spark.master", "local") \
    .getOrCreate()
```

## DataFrames: Structured Data Processing

### What is a DataFrame?
A DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as structured data files, tables in Hive, external databases, or existing RDDs.

### Creating DataFrames

**1. From RDDs:**
   You can convert an existing RDD of case classes (Scala), Row objects, or tuples to a DataFrame.
   ```scala
   // Scala: RDD of case classes
   case class Person(name: String, age: Long)
   val personRDD = spark.sparkContext.parallelize(Seq(Person("Alice", 30), Person("Bob", 25)))
   val personDF = personRDD.toDF()
   personDF.show()
   // +-----+---+
   // | name|age|
   // +-----+---+
   // |Alice| 30|
   // |  Bob| 25|
   // +-----+---+
   ```
   ```python
   # Python: RDD of Row objects or tuples
   from pyspark.sql import Row
   Person = Row("name", "age")
   person_rdd = spark.sparkContext.parallelize([Person("Alice", 30), Person("Bob", 25)])
   person_df = spark.createDataFrame(person_rdd)
   person_df.show()
   # Or using toDF() after creating RDD of tuples
   person_rdd_tuples = spark.sparkContext.parallelize([("Alice", 30), ("Bob", 25)])
   person_df_from_tuples = person_rdd_tuples.toDF(["name", "age"])
   person_df_from_tuples.show()
   ```

**2. From Files (JSON, Parquet, CSV):**
   Spark SQL supports reading data from various file formats.
   ```scala
   // Scala
   val dfJson = spark.read.json("examples/src/main/resources/people.json")
   val dfParquet = spark.read.parquet("examples/src/main/resources/users.parquet")
   val dfCsv = spark.read.option("header", "true").option("inferSchema", "true").csv("examples/src/main/resources/people.csv")
   ```
   ```python
   # Python
   df_json = spark.read.json("examples/src/main/resources/people.json")
   df_parquet = spark.read.parquet("examples/src/main/resources/users.parquet")
   df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv("examples/src/main/resources/people.csv")
   ```

**3. From Hive Tables (if Spark is configured with Hive support):**
   ```scala
   // val hiveDF = spark.read.table("my_hive_table")
   // val hiveDF = spark.sql("SELECT * FROM my_hive_table")
   ```

**4. From Relational Databases via JDBC:**
   ```scala
   // Scala
   val jdbcDF = spark.read
     .format("jdbc")
     .option("url", "jdbc:postgresql:dbserver")
     .option("dbtable", "schema.tablename")
     .option("user", "username")
     .option("password", "password")
     .load()
   ```

### Schema Management
*   **Schema Inference:** For self-describing formats like Parquet and JSON, Spark can automatically infer the schema.
*   **Programmatically Defining a Schema:** For formats like CSV or when converting RDDs, you might need to define the schema explicitly using `StructType` and `StructField`.
    ```scala
    // Scala
    import org.apache.spark.sql.types._
    val customSchema = StructType(Array(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("city", StringType, true)
    ))
    // val dfWithSchema = spark.read.schema(customSchema).csv("path/to/data.csv")
    ```
    ```python
    # Python
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    custom_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])
    # df_with_schema = spark.read.schema(custom_schema).csv("path/to/data.csv")
    ```
*   **`printSchema()`:** A useful method to display the schema of a DataFrame.
    ```scala
    dfJson.printSchema()
    // root
    //  |-- age: long (nullable = true)
    //  |-- name: string (nullable = true)
    ```

### Common DataFrame Operations (Transformations)
DataFrames support a rich set of transformations.

*   **`select(cols: Column*)` / `selectExpr(exprs: String*)`:** Selects a set of columns.
    ```scala
    personDF.select("name", "age").show()
    personDF.select(col("name"), col("age") + 1).show() // Using col() for expressions
    ```
    ```python
    from pyspark.sql.functions import col
    person_df.select("name", "age").show()
    person_df.select(col("name"), (col("age") + 1).alias("age_plus_one")).show()
    ```
*   **`filter(condition: Column)` / `where(condition: String)`:** Filters rows based on a condition.
    ```scala
    personDF.filter(col("age") > 25).show()
    personDF.where("age > 25").show()
    ```
*   **`groupBy(cols: Column*)` / `agg(exprs: Map[String, String])` or `agg(expr: Column, exprs: Column*)`:** Groups the DataFrame using the specified columns and then runs aggregations.
    ```scala
    // Scala
    import org.apache.spark.sql.functions._
    // Assume df has columns "department", "salary"
    // df.groupBy("department").agg(avg("salary"), max("age")).show()
    ```
    ```python
    # Python
    from pyspark.sql.functions import avg, max
    # df.groupBy("department").agg(avg("salary"), max("age")).show()
    ```
*   **`join(right: Dataset[_], joinExprs: Column, joinType: String)`:** Joins with another DataFrame.
    ```scala
    // val joinedDF = df1.join(df2, df1("id") === df2("id"), "inner")
    ```
*   **`orderBy(sortExprs: Column*)` / `sort(sortExprs: Column*)`:** Sorts the DataFrame by specified columns.
    ```scala
    personDF.orderBy(col("age").desc).show()
    ```
*   **`withColumn(colName: String, col: Column)`:** Adds a new column or replaces an existing one.
    ```scala
    personDF.withColumn("isAdult", col("age") >= 18).show()
    ```

### DataFrame Actions
Actions trigger computation and return results.
*   **`show(numRows: Int = 20, truncate: Boolean = true)`:** Displays the top `numRows` rows in a tabular format.
*   **`collect()`:** Returns all rows as an array/list to the driver. Use with caution.
*   **`count()`:** Returns the number of rows.
*   **`first()` / `head()` / `take(n)`:** Returns the first row or first `n` rows.

## Datasets: Type-Safe Structured Data Processing (Scala/Java)

A Dataset is a distributed collection of data. Dataset is a newer interface, added in Spark 1.6, which provides the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL’s optimized execution engine.

### What is a Dataset?
`Dataset[T]` is a typed interface available in Scala and Java. DataFrames are essentially `Dataset[Row]`, where `Row` is a generic untyped JVM object. Datasets leverage encoders to convert between JVM objects and Spark's internal Tungsten binary format.

### Benefits of Datasets
*   **Compile-time Type Safety:** Catch errors at compile time rather than runtime (unlike DataFrames when accessing columns by string name, or RDDs with incorrect types).
*   **Optimized Execution using Encoders:** Encoders provide efficient serialization/deserialization and allow Tungsten to operate directly on serialized data, avoiding expensive JVM object creation.

### Creating Datasets
```scala
// Scala
case class Person(name: String, age: Long)
val personDS = Seq(Person("Andy", 32)).toDS()
personDS.show()

val primitiveDS = Seq(1, 2, 3).toDS()
primitiveDS.map(_ + 1).show() // Type-safe map operation

// Convert DataFrame to Dataset
val personDF = spark.read.json("examples/src/main/resources/people.json")
val typedPersonDS = personDF.as[Person]
```

Java Datasets typically involve creating Java Beans and using `Encoders`.

## Running SQL Queries Programmatically

Spark SQL allows you to run SQL queries directly on DataFrames/Datasets by first registering them as a temporary view.

```scala
// Scala
personDF.createOrReplaceTempView("people")
val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
teenagersDF.show()
```
```python
# Python
person_df.createOrReplaceTempView("people")
teenagers_df = spark.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
teenagers_df.show()
```
Global temporary views are shared across sessions and are tied to a system preserved database `global_temp`.

## Interoperating with RDDs

Spark SQL integrates seamlessly with RDDs.
*   **DataFrame/Dataset to RDD:** `val myRDD = myDataFrame.rdd`
*   **RDD to DataFrame/Dataset:** As shown in DataFrame creation (`toDF()`, `createDataFrame()`).

## Data Sources API: Loading and Saving Data

Spark SQL provides a rich API for reading from and writing to various data sources.

### Generic `load()` and `save()` Methods
These methods allow you to specify the format and options explicitly.
```scala
// Scala - Reading
val usersDF = spark.read.format("parquet").load("users.parquet")
val peopleDF = spark.read.format("json").option("multiLine", "true").load("people.json")

// Scala - Writing
usersDF.write.format("orc").option("compression", "snappy").save("users.orc")
```

### Specific Format Methods
Convenience methods for common formats:
*   **Parquet:** Default format, columnar, good compression and performance. `spark.read.parquet()`, `df.write.parquet()`
*   **JSON:** `spark.read.json()`, `df.write.json()`
*   **CSV:** `spark.read.option("header", "true").csv()`, `df.write.option("header", "true").csv()`
*   **JDBC:** Read from/write to relational databases.
*   **Hive:** `spark.read.table()`, `df.write.saveAsTable()`

### Save Modes
Control behavior when saving data if the target location already exists:
*   `SaveMode.ErrorIfExists` (default): Throws an error.
*   `SaveMode.Append`: Adds data to the existing location.
*   `SaveMode.Overwrite`: Overwrites existing data.
*   `SaveMode.Ignore`: Silently does nothing if data already exists.

### Partitioning Data on Write
`df.write.partitionBy("year", "month").parquet("path")` can significantly improve query performance by pruning unnecessary directories.

## User-Defined Functions (UDFs) and Aggregate Functions (UDAFs)

### User-Defined Functions (UDFs)
UDFs allow you to define custom column-based functions.
```scala
// Scala
val toUpperUDF = udf((s: String) => s.toUpperCase())
spark.udf.register("toUpperSQL", (s: String) => s.toUpperCase())

personDF.withColumn("upperName", toUpperUDF(col("name"))).show()
spark.sql("SELECT toUpperSQL(name) FROM people").show()
```
```python
# Python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

to_upper_udf_py = udf(lambda s: s.upper() if s else None, StringType())
spark.udf.register("toUpperSQLPy", lambda s: s.upper() if s else None, StringType())

person_df.withColumn("upperName", to_upper_udf_py(col("name"))).show()
spark.sql("SELECT toUpperSQLPy(name) FROM people").show()
```
**Note:** UDFs in Python can have performance overhead due to data movement between JVM and Python interpreter. Scala/Java UDFs are generally more performant.

### User-Defined Aggregate Functions (UDAFs)
UDAFs allow you to define custom aggregation logic. This is more advanced and involves implementing specific interfaces (`UserDefinedAggregateFunction` or `Aggregator` in Scala).

## Optimizations in Spark SQL: Catalyst and Tungsten (Overview)

Spark SQL achieves high performance through two key components:

### The Catalyst Optimizer
Catalyst is Spark SQL's extensible query optimizer. It translates DataFrame/Dataset operations and SQL queries into an optimized logical and physical plan for execution. It applies a series of rule-based and cost-based optimizations, such as predicate pushdown, column pruning, and join reordering.

**Phases:**
1.  **Analysis:** Resolves references to tables and columns.
2.  **Logical Optimization:** Applies standard rule-based optimizations to the logical plan.
3.  **Physical Planning:** Selects one or more physical plans from the logical plan and chooses the best one based on cost estimation.
4.  **Code Generation:** Generates efficient Java bytecode for the selected physical plan (often using Tungsten).

### The Tungsten Execution Engine
Tungsten focuses on optimizing Spark jobs for CPU and memory efficiency. Key features include:
*   **Off-Heap Memory Management:** Reduces garbage collection overhead by managing memory explicitly.
*   **Cache-Aware Computation:** Algorithms and data structures that are aware of memory hierarchy.
*   **Whole-Stage Code Generation:** Generates bytecode for entire stages of a query rather than individual operators, reducing virtual function calls and improving instruction pipelining.

Together, Catalyst and Tungsten make DataFrames and Datasets significantly faster than using RDDs directly for many structured data workloads.

## Chapter Summary

This chapter illuminated Spark SQL and its primary abstractions, DataFrames and Datasets. We covered their creation from diverse data sources, a wide array of operations for data manipulation and querying, schema management, and the ability to seamlessly mix programmatic APIs with SQL. The power of the Data Sources API for I/O, along with the extensibility provided by UDFs and UDAFs, was also explored. Finally, we gained a high-level appreciation for the Catalyst optimizer and Tungsten execution engine, which are the engines behind Spark SQL's impressive performance. With this knowledge, you are well-equipped to tackle complex structured data processing tasks using Spark. The next chapter will delve into Spark Streaming for real-time data processing.

---

## Further Reading and Resources

For a deeper dive into the topics covered in this chapter and to explore related concepts, please refer to the following resources:

*   **Consolidated References:** For a comprehensive list of official documentation, books, and community resources, see the [References](../references.md) section of this book.
*   **Glossary of Terms:** To understand key terminology used in Apache Spark, consult the [Glossary](../glossary.md).
*   **Book Index:** For a detailed index of topics, refer to the [Index](../index.md).

*(Specific links from `references.md` or pointers to other chapters can be added here if highly relevant to this specific chapter's content.)*
