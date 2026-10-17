# SQL vs Spark SQL vs T-SQL | Data Engineering with Microsoft Fabric

**From a requirement found in Data Engineer job postings to the implementation of a practical solution using Portugal's open data.**

## 📑 Summary

1. [Introduction](#1-introduction-a-skill-that-appears-in-data-engineer-job-postings)
2. [Project Objectives](#2-project-objectives)
3. [SQL vs Spark SQL vs T-SQL](#3-sql-vs-spark-sql-vs-t-sql)
   - [SQL — Structured Query Language](#31-sql--structured-query-language)
   - [Spark SQL](#32-spark-sql)
   - [T-SQL — Transact-SQL](#33-t-sql--transact-sql)
4. [What Is Microsoft Fabric?](#4-what-is-microsoft-fabric)
5. [Business Problem](#5-business-problem)
6. [Technical Challenge](#6-challenge--the-technical-challenge)
7. [Demo — Implementation in Microsoft Fabric](#7-demo--implementation-in-microsoft-fabric)
   - [Create a Workspace](#71-create-a-workspace)
   - [Create a Lakehouse](#72-create-a-lakehouse)
   - [Upload the Dataset](#73-upload-the-dataset)
   - [Load the Dataset into a Table](#74-load-the-dataset-into-a-table)
   - [Identify and Resolve Issues](#75-identify-and-resolve-issues)
   - [Create a Notebook](#76-create-a-notebook)
   - [Spark SQL Queries](#77-spark-sql-queries)
   - [T-SQL Queries](#78-t-sql-queries)
   - [Comparing the Results](#79-comparing-the-results)
8. [Analysis Results and Conclusions](#8-analysis-results-and-conclusions)
9. [YouTube Demonstration Video](#9-youtube-demonstration-video)
10. [References and Documentation](#10-references-and-documentation)
11. [Author and Purpose](#-author-and-purpose)

## 1. Introduction: A Skill That Appears in Data Engineer Job Postings

When looking for job opportunities in the data field, particularly for **Data Engineer** positions, it is common to find requirements such as **knowledge of SQL, T-SQL, and Spark SQL**.

These requirements raise an important question for anyone learning or developing skills in this field:

**If SQL, Spark SQL, and T-SQL can all be used to query data, what is the difference between them, and when should we use each one?**

The answer becomes clearer when we move beyond theory and examine a real-world problem.

In this project, we will use a **public dataset from Portugal**, made available through the **National Open Data Portal**, to explore these technologies and understand how they can be applied in the day-to-day work of a Data Engineer.

The goal is to understand how data is made available and write queries that answer business questions and support business decision-making.

---

## 2. Project Objectives

The objectives of this project are:

- Understand the differences between SQL, Spark SQL, and T-SQL.
- Learn about Microsoft Fabric and the Lakehouse concept.
- Work with a real public dataset.
- Create a workspace and a Lakehouse in Microsoft Fabric.
- Load a CSV file into a Delta table.
- Explore and analyze data using Spark SQL in a notebook.
- Run equivalent queries using T-SQL through a notebook.
- Identify and resolve common issues during data ingestion and querying.
- Demonstrate how these skills can be applied to a real Data Engineering task.

## 3. SQL vs Spark SQL vs T-SQL

Before starting the implementation, we need to understand the role of each technology.

### 3.1. SQL — Structured Query Language

SQL is a language used to query, transform, and manipulate data in relational database systems.

It is one of the fundamental skills for data professionals, allowing them to perform operations such as:

- Filtering records with `WHERE`.
- Grouping data with `GROUP BY`.
- Calculating metrics with `SUM()`, `COUNT()`, and `AVG()`.
- Combining tables with `JOIN`.
- Sorting results with `ORDER BY`.
- Creating analytical queries with window functions.

**When should it be used?**

SQL is the foundation for working with relational data, whether we are developing reports, validating data, building transformations, or analyzing business information.

It is important to understand that SQL is a language, not a specific execution engine. Different platforms implement their own dialects and features.

Documentation: [Microsoft SQL Documentation](https://learn.microsoft.com/en-us/sql/)

### 3.2. Spark SQL

Spark SQL is the Apache Spark module that allows users to work with structured data through SQL queries, DataFrames, and other programming interfaces.

Unlike traditional SQL usage in a relational database engine, Spark SQL integrates with the Apache Spark processing engine, which can distribute processing across multiple executors.

It is particularly useful in Data Engineering scenarios involving:

- Large volumes of data.
- Complex transformations.
- Distributed processing.
- Integration between SQL and Python.
- Processing tables in formats such as Delta Lake and Parquet.

The query syntax is similar to traditional SQL. The main difference lies in the execution context: in this project, queries will be executed by the Spark engine in a Microsoft Fabric notebook.

**When should it be used?**

Spark SQL is a suitable option when we need to transform and analyze data in a Spark processing environment, especially in data pipelines and engineering tasks that benefit from Python integration and distributed processing.

Documentation: [Apache Spark SQL](https://spark.apache.org/sql/)

### 3.3. T-SQL — Transact-SQL

T-SQL is an extension of the SQL language used in Microsoft products, including SQL Server and other compatible services.

In addition to common SQL operations, it offers specific features such as variables, control-flow structures, stored procedures, and its own extensions for querying and managing data.

**When should it be used?**

T-SQL is particularly relevant in environments based on SQL Server, Azure SQL, and Microsoft tools that provide compatible SQL interfaces.

In this project, it will be used through the SQL analytics endpoint of the Microsoft Fabric Lakehouse.

Documentation: [T-SQL Reference](https://learn.microsoft.com/en-us/sql/t-sql/language-reference)

---

## 4. What Is Microsoft Fabric?

Microsoft Fabric is a data analytics platform that brings together several capabilities, including Data Engineering, Data Factory, Data Warehouse, Real-Time Intelligence, and Power BI.

The platform allows users to develop data solutions in an integrated environment, reducing the need to manage separate tools for data ingestion, storage, transformation, and analysis.

In this project, we will use three main components.

### Workspace

The workspace is where we organize project artifacts, such as Lakehouses, notebooks, pipelines, and reports.

### Lakehouse

The Lakehouse combines characteristics of a data lake with capabilities for managing and querying structured data.

In Microsoft Fabric, the Lakehouse can store files and tables managed using formats such as Delta Lake.

### Notebook

The notebook allows us to develop and execute code in cells, using technologies such as Spark SQL and T-SQL.

---

## 5. Business Problem

### Context

Medical training planning is an important component of human resource management in the healthcare sector.

To support this planning, it is useful to track how many doctors complete specialist training, which specialties are represented, and how results evolve over the years and across institutions.

Public data allows us to analyze part of this reality by transforming annual records into indicators that can support exploration and planning.

### Problem

**How can we analyze the evolution of specialist medical training in Portugal and identify the specialties and institutions with the highest number of completions?**

The goal is to transform data provided in CSV format into a structure that allows us to answer specific questions.

### Business Questions

1. How many doctors completed specialist training each year?
2. How did the number of completions evolve between 2020 and 2025?
3. Which five specialties have the highest cumulative number of completions?
4. Which institutions have the highest training volumes?
5. In which years did the number of completions increase or decrease compared with the previous year?
6. How can we make these indicators available through different query engines?

### Data Source

- **Source:** Portugal's National Open Data Portal.
- **Dataset:** Medical Training | Newly Qualified Specialists.
- **Description:** Number of doctors who successfully completed specialist training, by specialty and institution, considering the first regular examination period and the second special examination period.
- **Original format:** CSV.
- **Period analyzed:** 2020–2025, according to the data in the file used for this project.

Source: [National Open Data Portal](https://dados.gov.pt/)

> The data represents training completions, not the total number of doctors available to work. On its own, it cannot determine recruitment needs, professional shortages, or the actual distribution of doctors within Portugal's National Health Service (SNS).

---

## 6. Challenge — The Technical Challenge

The challenge is to implement a small data analytics solution, from ingestion to querying.

The project will be divided into the following stages:

1. Create the working environment in Microsoft Fabric.
2. Create a Lakehouse.
3. Upload the CSV file.
4. Convert the data into a queryable table.
5. Identify and resolve potential ingestion or data-typing issues.
6. Create a notebook.
7. Run queries using Spark SQL.
8. Run equivalent queries using T-SQL.
9. Document the solution and present the demonstration in a video.

---

## 7. Demo — Implementation in Microsoft Fabric

### 7.1. Create a Workspace

Access [Microsoft Fabric](https://app.fabric.microsoft.com/) and create a workspace to organize the project components.

Suggested name:

`SQL-SparkSQL-TSql-Demo`

The workspace will be the starting point for creating the Lakehouse and notebook.

### 7.2. Create a Lakehouse

Inside the workspace:

1. Select the option to create a new item.
2. Choose **Lakehouse**.
3. Set a name, for example, `LakehouseFormacaoMedica`.
4. Create the Lakehouse.

The Lakehouse will be the storage layer used to organize the project data.

### 7.3. Upload the Dataset

Use the CSV file provided by Portugal's National Open Data Portal.

In the Lakehouse:

1. Open the Files section.
2. Upload the CSV file.
3. Confirm that the file was uploaded successfully.

At this stage, the file is stored in the Lakehouse, but we still need to ensure that the data is available in a table for SQL queries.

### 7.4. Load the Dataset into a Table

Use the file-loading option to create a table from the CSV file.

Suggested name:

`medicos_recem_especialistas`

During ingestion, check:

- Whether the delimiter was identified correctly.
- Whether the column headers were interpreted correctly.
- Whether the numeric column is stored using a numeric data type.
- Whether year values are consistent.
- Whether there are null values or unexpected rows.

### 7.5. Identify and Resolve Issues

When ingesting CSV files, issues may arise involving headers, delimiters, data types, column names, or duplicate records.

A practical troubleshooting approach consists of:

1. Inspecting the table schema.
2. Checking a sample of the data.
3. Identifying columns that should be numeric but were interpreted as text.
4. Confirming that the aggregated values are consistent with the original file.

### 7.6. Create a Notebook

In the workspace, create a notebook and associate it with the Lakehouse.

Suggested name:

`medicos_recem_especialistas`

The notebook will be used to run Spark SQL queries and explore the results.

### 7.7. Spark SQL Queries

#### A. Total Number of Doctors per Year

```sql
%%sql

SELECT
    periodo,
    SUM(medicos_recem_especialistas) AS total_medicos
FROM medicos_recem_especialistas
GROUP BY periodo
ORDER BY periodo;
```

This query allows us to analyze the annual evolution of the number of doctors who completed their training.

#### B. Top 5 Specialties

```sql
%%sql

SELECT
    especialidade,
    SUM(medicos_recem_especialistas) AS total_medicos
FROM medicos_recem_especialistas
GROUP BY especialidade
ORDER BY total_medicos DESC
LIMIT 5;
```

This analysis identifies the specialties with the highest cumulative number of completions during the period.

#### C. Annual Percentage Change

```sql
%%sql

WITH totais_anuais AS (
    SELECT
        periodo,
        SUM(medicos_recem_especialistas) AS total_medicos
    FROM medicos_recem_especialistas
    GROUP BY periodo
),
comparacao AS (
    SELECT
        periodo,
        total_medicos,
        LAG(total_medicos) OVER (
            ORDER BY periodo
        ) AS total_ano_anterior
    FROM totais_anuais
)
SELECT
    periodo,
    total_medicos,
    total_ano_anterior,
    ROUND(
        100.0 * (total_medicos - total_ano_anterior)
        / NULLIF(total_ano_anterior, 0),
        2
    ) AS variacao_percentual
FROM comparacao
ORDER BY periodo;
```

This query introduces the `LAG()` window function, which allows us to compare each year's total with the previous year's value.

### 7.8. T-SQL Queries

The following queries should be executed through the Lakehouse SQL analytics endpoint, not in a Spark SQL notebook cell.

The schema and table name should be confirmed in the endpoint. In these examples, we use `dbo.medicos_recem_especialistas`.

#### A. Total Number of Doctors per Year

```sql
SELECT
    [periodo],
    SUM([medicos_recem_especialistas]) AS total_medicos
FROM [dbo].[medicos_recem_especialistas]
GROUP BY [periodo]
ORDER BY [periodo];
```

The logic is equivalent to the Spark SQL query, but execution takes place through the SQL endpoint.

#### B. Top 5 Specialties

```sql
SELECT TOP 5
    [especialidade],
    SUM([medicos_recem_especialistas]) AS total_medicos
FROM [dbo].[medicos_recem_especialistas]
GROUP BY [especialidade]
ORDER BY total_medicos DESC;
```

Here, we use `TOP 5` instead of `LIMIT 5`, illustrating a syntax difference between the two contexts.

#### C. Annual Percentage Change

```sql
WITH totais_anuais AS (
    SELECT
        [periodo],
        SUM([medicos_recem_especialistas]) AS total_medicos
    FROM [dbo].[medicos_recem_especialistas]
    GROUP BY [periodo]
),
comparacao AS (
    SELECT
        [periodo],
        total_medicos,
        LAG(total_medicos) OVER (
            ORDER BY [periodo]
        ) AS total_ano_anterior
    FROM totais_anuais
)
SELECT
    [periodo],
    total_medicos,
    total_ano_anterior,
    ROUND(
        100.0 * (total_medicos - total_ano_anterior)
        / NULLIF(total_ano_anterior, 0),
        2
    ) AS variacao_percentual
FROM comparacao
ORDER BY [periodo];
```

This query uses `LAG()` and `NULLIF()` to calculate the percentage change while protecting against division by a zero denominator.

### 7.9. Comparing the Results

After running the queries in both contexts, compare the results.

| Analysis | Spark SQL | T-SQL |
|---|---|---|
| Total per year | `GROUP BY` and `SUM()` | `GROUP BY` and `SUM()` |
| Top 5 specialties | `LIMIT 5` | `TOP 5` |
| Year-over-year comparison | `LAG()` | `LAG()` |
| Execution environment | Spark notebook | SQL analytics endpoint / T-SQL notebook |

The most noticeable differences in this demonstration are the interface, the execution context, and some syntax details.

We should not assume that an equivalent query will always have the same performance in both engines. Performance depends on the data volume, implementation, optimizations, and execution conditions.

---

## 8. Analysis Results and Conclusions

Based on the file used in this project, the reference indicators are:

- **9,346** cumulative training completions between 2020 and 2025.
- **1,688** completions recorded in 2025.
- **50** distinct specialties.
- **260** distinct institution names.

The annual trend shows an approximate increase of 16.9% between 2020 and 2025, with decreases in 2023 and 2024, followed by an increase in 2025.

General Practice and Family Medicine has the highest cumulative total among the specialties analyzed, with 2,689 training completions during the period.

These indicators allow us to explore the evolution of medical training, identify patterns, and formulate additional questions to support planning.

### What Did We Learn?

This project demonstrates that:

- SQL is a cross-functional skill in the data field.
- Spark SQL allows us to query and transform data within the Apache Spark ecosystem.
- T-SQL allows us to work with the SQL engine provided by the Lakehouse endpoint.
- Microsoft Fabric integrates storage, processing, and querying within a single platform.

The main conclusion is that knowing a language's syntax is not enough. A Data Engineer must know how to choose the appropriate execution context and understand how data moves from its source to the final result.

---

## 9. YouTube Demonstration Video

🎥 **SQL vs Spark SQL vs T-SQL | Practical Demo with Microsoft Fabric**

**Watch the video:** Add the YouTube video URL.

---

## 10. References and Documentation

- [National Open Data Portal — dados.gov.pt](https://dados.gov.pt/)
- [Microsoft Fabric — Official Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Microsoft Fabric Lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- [Apache Spark SQL](https://spark.apache.org/sql/)
- [T-SQL — Official Documentation](https://learn.microsoft.com/en-us/sql/t-sql/language-reference)
- [Notebooks in Microsoft Fabric](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook)

---

- [Microsoft Fabric](https://learn.microsoft.com/fabric/)
- [Apache Spark SQL Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [T-SQL Documentation](https://learn.microsoft.com/sql/t-sql/language-reference)
- [Microsoft Fabric Lakehouse Documentation](https://learn.microsoft.com/fabric/data-engineering/lakehouse-overview)
- [Microsoft Fabric Notebook Documentation](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook)

---

## 👤 Author and Purpose

**Author:** To be completed

**LinkedIn:** To be completed

**GitHub:**

The goal is to document technical knowledge through practical projects, connecting concepts found in documentation and Data Engineer job requirements with real-world work scenarios.

If you find this project useful, you can leave a ⭐ on the repository and share suggestions or improvements.

**Thank you for following this project!**
