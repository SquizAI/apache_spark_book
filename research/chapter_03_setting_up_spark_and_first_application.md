# Chapter 3: Setting Up Spark and Your First Application

## Topic

This chapter provides a practical guide to getting Apache Spark up and running on your system. We'll cover downloading Spark, configuring it for local development, and setting up a basic standalone cluster. The centerpiece of this chapter is writing and running your very first Spark application – the classic "Word Count" example – in Scala, Python, and Java. We will also explore how to submit applications using `spark-submit` and interact with Spark using its interactive shells.

## Keywords

Apache Spark Installation, Download Spark, Spark Setup, Local Mode, Standalone Cluster Mode, Spark Configuration, `SPARK_HOME`, `spark-env.sh`, `spark-defaults.conf`, Java Development Kit (JDK), Scala, Python, Word Count, First Spark Application, `SparkContext`, `SparkSession`, `spark-submit`, Spark Shell, `pyspark`, `spark-shell`, Spark UI.

## Structure

1.  [Prerequisites for Installing Spark](#prerequisites-for-installing-spark)
    *   Java Development Kit (JDK)
    *   Scala (Optional, for Scala development)
    *   Python (For PySpark development)
    *   Understanding Your Operating System
2.  [Downloading Apache Spark](#downloading-apache-spark)
    *   Choosing the Right Spark Release
    *   Downloading Pre-built Binaries
    *   Verifying the Download
3.  [Setting Up Spark in Local Mode](#setting-up-spark-in-local-mode)
    *   Unpacking Spark
    *   Setting Environment Variables (`SPARK_HOME`, `PATH`)
    *   Running Spark Shells (Scala and PySpark) in Local Mode
    *   Understanding Local Mode (`local[*]`)
4.  [Writing Your First Spark Application: Word Count](#writing-your-first-spark-application-word-count)
    *   The Word Count Problem Explained
    *   Creating an Input Text File
    *   Word Count in Scala
        *   Project Setup (SBT or Maven - brief overview)
        *   Scala Code
    *   Word Count in Python (PySpark)
        *   Python Script
    *   Word Count in Java
        *   Project Setup (Maven or Gradle - brief overview)
        *   Java Code
5.  [Running Your Application with `spark-submit`](#running-your-application-with-spark-submit)
    *   Basic `spark-submit` Syntax
    *   Key `spark-submit` Options (`--class`, `--master`, `--deploy-mode`, application JAR/Python file, application arguments)
    *   Submitting the Scala Word Count Application
    *   Submitting the Python Word Count Application
    *   Submitting the Java Word Count Application
6.  [Interacting with Spark Shells](#interacting-with-spark-shells)
    *   Scala Shell (`spark-shell`)
    *   Python Shell (`pyspark`)
    *   Executing Word Count Logic Interactively
7.  [Setting Up a Basic Standalone Spark Cluster](#setting-up-a-basic-standalone-spark-cluster)
    *   Overview of Standalone Mode Components (Master, Worker)
    *   Starting the Master
    *   Starting Worker(s)
    *   Configuration using `conf/spark-env.sh`
    *   Submitting an Application to the Standalone Cluster
    *   Monitoring with the Spark Web UI
8.  [Understanding the Spark Web UI](#understanding-the-spark-web-ui)
    *   Accessing the UI (Driver UI, Master UI, Worker UI)
    *   Key Information Displayed (Jobs, Stages, Tasks, Storage, Executors)
9.  [Chapter Summary](#chapter-summary)

---

## Prerequisites for Installing Spark

Before you download and install Spark, ensure you have the necessary prerequisites:

### Java Development Kit (JDK)
Spark is written in Scala, which runs on the Java Virtual Machine (JVM). Therefore, a JDK is essential. Spark typically requires Java 8 or later. You can check your Java version using `java -version`.
*   **Installation:** Download from Oracle, OpenJDK, or Adoptium.

### Scala (Optional, for Scala development)
If you plan to write Spark applications in Scala or use the Scala shell, you'll need Scala installed. The Scala version should be compatible with the Spark version you download (e.g., Spark 3.x usually works well with Scala 2.12 or 2.13).
*   **Installation:** Download from the official Scala website or use package managers like SDKMAN! or Homebrew.

### Python (For PySpark development)
For PySpark applications, you need Python installed (Python 3.6+ is generally recommended for recent Spark versions). Ensure `pip` is also available for managing Python packages.
*   **Installation:** Download from Python.org or use system package managers.

### Understanding Your Operating System
Spark can run on Windows, macOS, and Linux. The setup process might have slight variations depending on your OS, particularly for setting environment variables.

## Downloading Apache Spark

1.  **Navigate to the Official Apache Spark Website:** Go to [spark.apache.org/downloads.html](https://spark.apache.org/downloads.html).
2.  **Choose the Spark Release:** Select the desired Spark version from the dropdown. It's generally recommended to use the latest stable release.
3.  **Choose a Package Type:** For most users, selecting a pre-built package for a specific Hadoop version (or without Hadoop if you manage it separately) is the easiest. If you don't have a Hadoop cluster or are unsure, a package like "Pre-built for Apache Hadoop 3.3 and later" is a good general choice.
4.  **Download the TGZ File:** Click the download link for the `.tgz` file (e.g., `spark-3.5.0-bin-hadoop3.tgz`).
5.  **Verify the Download (Optional but Recommended):** Use checksums (SHA-512) or GPG signatures provided on the download page to verify the integrity of the downloaded file.

## Setting Up Spark in Local Mode

Local mode is the simplest way to run Spark on a single machine for development and testing. It doesn't require a cluster setup.

1.  **Unpack Spark:**
    Extract the downloaded `.tgz` file. On Linux/macOS:
    ```bash
    tar -xzf spark-3.x.x-bin-hadoop3.x.tgz
    cd spark-3.x.x-bin-hadoop3.x
    ```
    On Windows, use a tool like 7-Zip.
    The extracted directory will be referred to as `SPARK_HOME`.

2.  **Set Environment Variables:**
    *   **`SPARK_HOME`:** Set this variable to the path where you extracted Spark.
    *   **`PATH`:** Add `$SPARK_HOME/bin` (or `%SPARK_HOME%\bin` on Windows) to your system's `PATH` variable so you can run Spark commands like `spark-shell`, `pyspark`, and `spark-submit` from any directory.

    **Example (Linux/macOS - add to `.bashrc` or `.zshrc`):**
    ```bash
    export SPARK_HOME=/path/to/spark-3.x.x-bin-hadoop3.x
    export PATH=$SPARK_HOME/bin:$PATH
    # Also ensure JAVA_HOME is set
    export JAVA_HOME=/path/to/your/java_installation
    ```
    Remember to source your shell configuration file (e.g., `source ~/.bashrc`) or open a new terminal.

3.  **Running Spark Shells in Local Mode:**
    *   **Scala Shell:** Open a terminal and type `spark-shell`.
    *   **Python Shell:** Open a terminal and type `pyspark`.
    If successful, you'll see the Spark logo and a shell prompt.

4.  **Understanding Local Mode (`local[*]`):
    When you run `spark-shell` or `pyspark` without specifying a `--master` URL, it defaults to local mode. `local` uses one thread. `local[K]` uses K worker threads. `local[*]` uses as many worker threads as logical cores on your machine. This allows parallel processing even on a single machine.

## Writing Your First Spark Application: Word Count

The "Word Count" application reads a text file, counts the occurrences of each word, and outputs the counts.

### The Word Count Problem Explained
Given a body of text, the goal is to produce a list of unique words and how many times each word appears.

### Creating an Input Text File
Create a simple text file, say `input.txt`, with some content:
```
Hello Spark Hello World
Spark is fast and Spark is easy
Learning Spark is fun
```

### Word Count in Scala

**Project Setup (SBT Example for `build.sbt`):**
```scala
name := "SparkWordCountScala"
version := "1.0"
scalaVersion := "2.12.15" // Match your Spark's Scala version

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0" // Use your Spark version
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
```

**Scala Code (`src/main/scala/WordCount.scala`):**
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: WordCount <file>")
      System.exit(1)
    }

    val spark = SparkSession.builder.appName("Scala Word Count").getOrCreate()
    val sc = spark.sparkContext // Get SparkContext from SparkSession

    // Read input file
    val lines = sc.textFile(args(0))

    // Perform word count
    val words = lines.flatMap(line => line.split(" "))
    val wordPairs = words.map(word => (word, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)

    // Print results to console
    wordCounts.collect().foreach(println)

    // Or save to a file (output directory will be created)
    // wordCounts.saveAsTextFile("output_scala")

    spark.stop()
  }
}
```

### Word Count in Python (PySpark)

**Python Script (`word_count.py`):**
```python
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: word_count.py <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

    # Read input file
    lines = spark.sparkContext.textFile(sys.argv[1])

    # Perform word count
    words = lines.flatMap(lambda line: line.split(" "))
    wordPairs = words.map(lambda word: (word, 1))
    wordCounts = wordPairs.reduceByKey(lambda a, b: a + b)

    # Print results to console
    for wc in wordCounts.collect():
        print(wc)

    # Or save to a file
    # wordCounts.saveAsTextFile("output_python")

    spark.stop()
```

### Word Count in Java

**Project Setup (Maven Example for `pom.xml` - add dependencies):**
```xml
<dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.12</artifactId> <!-- Adjust Scala version if needed -->
        <version>3.5.0</version> <!-- Use your Spark version -->
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.12</artifactId>
        <version>3.5.0</version>
    </dependency>
</dependencies>
```

**Java Code (`src/main/java/com/example/JavaWordCount.java`):**
```java
package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JavaWordCount {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("JavaWordCount")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        // counts.saveAsTextFile("output_java");

        spark.stop();
    }
}
```

## Running Your Application with `spark-submit`

`spark-submit` is the universal script for submitting Spark applications to any cluster manager.

**Basic Syntax:**
`spark-submit [options] <app jar | python file> [app arguments]`

**Key Options:**
*   `--class <main-class>`: Your application's main class (for Java/Scala).
*   `--master <master-url>`: The master URL for the cluster (e.g., `local[*]`, `spark://host:port`, `yarn`, `mesos://host:port`, `k8s://https://host:port`).
*   `--deploy-mode <deploy-mode>`: Whether to deploy your driver on the worker nodes (`cluster`) or locally as an external client (`client`) (default: `client`).
*   `--name <application-name>`: A name for your application.
*   `--conf <key>=<value>`: Arbitrary Spark configuration property.
*   `--jars <comma-separated-list-of-jars>`: Comma-separated list of local jars to include on the driver and executor classpaths.
*   `--py-files <comma-separated-list-of-python-files>`: Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python applications.

**Submitting the Scala Word Count Application:**
1.  Package your Scala application into a JAR (e.g., using `sbt package`).
2.  Submit:
    ```bash
    spark-submit \
      --class WordCount \
      --master local[*] \
      target/scala-2.12/sparkwordcountscala_2.12-1.0.jar \
      input.txt
    ```

**Submitting the Python Word Count Application:**
```bash
spark-submit \
  --master local[*] \
  word_count.py \
  input.txt
```

**Submitting the Java Word Count Application:**
1.  Package your Java application into a JAR (e.g., using `mvn package`).
2.  Submit:
    ```bash
    spark-submit \
      --class com.example.JavaWordCount \
      --master local[*] \
      target/your-java-word-count-jar-with-dependencies.jar \
      input.txt
    ```

## Interacting with Spark Shells

Spark shells provide an interactive way to learn Spark and analyze data.

### Scala Shell (`spark-shell`)
Starts a Scala REPL with a preconfigured `SparkSession` (named `spark`) and `SparkContext` (named `sc`).
```bash
spark-shell --master local[*]
```
Inside the shell:
```scala
val lines = sc.textFile("input.txt")
val words = lines.flatMap(line => line.split(" "))
val wordPairs = words.map(word => (word, 1))
val wordCounts = wordPairs.reduceByKey(_ + _)
wordCounts.collect().foreach(println)
```

### Python Shell (`pyspark`)
Starts a Python interpreter with a preconfigured `SparkSession` (named `spark`) and `SparkContext` (named `sc`).
```bash
pyspark --master local[*]
```
Inside the shell:
```python
lines = sc.textFile("input.txt")
words = lines.flatMap(lambda line: line.split(" "))
wordPairs = words.map(lambda word: (word, 1))
wordCounts = wordPairs.reduceByKey(lambda a, b: a + b)
for wc in wordCounts.collect():
  print(wc)
```

## Setting Up a Basic Standalone Spark Cluster

A standalone cluster involves a Spark master process and one or more Spark worker processes.

1.  **Overview:** The master is responsible for coordinating workers. Workers launch executors for applications.
2.  **Starting the Master:**
    ```bash
    cd $SPARK_HOME
    ./sbin/start-master.sh
    ```
    By default, the master UI is available at `http://localhost:8080`.

3.  **Starting Worker(s):**
    Connect a worker to the master (replace `MASTER_IP` and `MASTER_PORT` with your master's IP/hostname and port, usually 7077):
    ```bash
    ./sbin/start-worker.sh spark://MASTER_IP:MASTER_PORT
    ```
    You can start multiple workers on different machines, or multiple workers on the same machine (by specifying different ports using `--port` and different web UI ports using `--webui-port`).

4.  **Configuration using `conf/spark-env.sh`:**
    You can set Spark configuration options in `conf/spark-env.sh` (copy `conf/spark-env.sh.template` if it doesn't exist). For example:
    ```bash
    export SPARK_MASTER_HOST='your_master_ip_here'
    export SPARK_WORKER_CORES=2
    export SPARK_WORKER_MEMORY=2g
    ```

5.  **Submitting an Application to the Standalone Cluster:**
    Use `spark-submit` and set the `--master` URL to `spark://MASTER_IP:MASTER_PORT`.
    ```bash
    spark-submit \
      --class WordCount \
      --master spark://MASTER_IP:MASTER_PORT \
      /path/to/your/sparkwordcountscala_2.12-1.0.jar \
      input.txt
    ```

6.  **Monitoring with the Spark Web UI:**
    *   Master UI: `http://MASTER_IP:8080` shows workers, running applications, and completed applications.
    *   Worker UI: Accessible from the Master UI, shows running executors and completed tasks for that worker.
    *   Application UI: Accessible from the Master UI (for running apps) or history server (for completed apps, if configured). Shows jobs, stages, tasks, storage, etc. for a specific application. The default port for an application's driver UI is 4040.

## Understanding the Spark Web UI

The Spark Web UI is an invaluable tool for monitoring and debugging your Spark applications.

*   **Accessing:**
    *   For applications running in `local` mode or `client` deploy mode: `http://<driver-node>:4040` (default).
    *   Standalone Master: `http://<master-node>:8080`.
    *   YARN ResourceManager UI often links to Spark application UIs.
*   **Key Information:**
    *   **Jobs Tab:** Shows active and completed jobs, their stages, and progress.
    *   **Stages Tab:** Details about each stage, including tasks, shuffle read/write, and duration.
    *   **Tasks View (within Stages):** Information about individual tasks, including locality, duration, and any errors.
    *   **Storage Tab:** Information about cached RDDs and DataFrames (partitions, memory/disk usage).
    *   **Environment Tab:** Spark properties, system properties, and classpath.
    *   **Executors Tab:** List of active executors, their resources, tasks completed, and storage used.
    *   **SQL Tab (for Spark SQL queries):** Query plans and execution details.

## Chapter Summary

This chapter walked you through the essential steps to get started with Apache Spark. You've learned how to download and set up Spark in local mode, write a fundamental Word Count application in Scala, Python, and Java, and submit it using `spark-submit`. We also explored the interactive Spark shells and the basics of setting up and using a standalone Spark cluster. Finally, we introduced the Spark Web UI, a critical tool for monitoring your applications. With this foundation, you're ready to dive deeper into Spark's core concepts and APIs in the upcoming chapters.

---

## Further Reading and Resources

For a deeper dive into the topics covered in this chapter and to explore related concepts, please refer to the following resources:

*   **Consolidated References:** For a comprehensive list of official documentation, books, and community resources, see the [References](../references.md) section of this book.
*   **Glossary of Terms:** To understand key terminology used in Apache Spark, consult the [Glossary](../glossary.md).
*   **Book Index:** For a detailed index of topics, refer to the [Index](../index.md).

*(Specific links from `references.md` or pointers to other chapters can be added here if highly relevant to this specific chapter's content.)*
