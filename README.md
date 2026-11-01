# IT462 Big Data Project | UNSW-NB15 Network Traffic Analysis

<p align="center">
  <img alt="Scala" src="https://img.shields.io/badge/Scala-DC322F?style=for-the-badge&logo=scala&logoColor=white" />
  <img alt="Apache Spark" src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img alt="Spark SQL" src="https://img.shields.io/badge/Spark_SQL-2D3748?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img alt="Apache Parquet" src="https://img.shields.io/badge/Parquet-005A9C?style=for-the-badge&logo=apacheparquet&logoColor=white" />
  <img alt="CSV" src="https://img.shields.io/badge/CSV-217346?style=for-the-badge&logoColor=white" />
</p>

<p align="center"><strong>King Saud University · IT462 – Big Data</strong></p>

## Project Overview

This academic project explores large-scale network traffic data using **Apache Spark and Scala**. We work with the **UNSW-NB15 cybersecurity dataset** to prepare and analyze network records, compare normal and malicious traffic, and identify patterns associated with network attacks.

The project demonstrates a big-data workflow through data preprocessing, distributed analysis using **Resilient Distributed Datasets (RDDs)**, and structured analysis using **Spark SQL**.

## Dataset

- **Name:** UNSW-NB15
- **Source:** [UNSW Research – UNSW-NB15 Dataset](https://research.unsw.edu.au/projects/unsw-nb15-dataset)
- **Size:** Approximately 2.54 million network traffic records across four original CSV files
- **Attributes:** 49 columns in the dataset used for this project
- **Content:** Normal traffic and nine categories of network attacks

## Project Phases

### Phase 2 – Data Preprocessing

Using Spark DataFrames and Scala, we:
- Loaded and integrated the four UNSW-NB15 CSV files.
- Validated the dataset schema and inspected data quality.
- Examined missing and invalid values.
- Standardized attack-category labels and handled missing values according to their context.
- Prepared the data for subsequent distributed analysis.

**Code:** [`P2_DataPreprocessing.scala`](P2_DataPreprocessing.scala)

### Phase 3 – RDD Operations

We applied Spark RDD transformations and actions to analyze network traffic at scale, using operations such as `filter`, `map`, `reduceByKey`, `sortBy`, and `take`. These analyses explore patterns in attack-related traffic and network services.

**Code:** [`Phase3_RDD/`](Phase3_RDD/)

### Phase 4 – Spark SQL Analysis

We used Spark SQL to query the preprocessed dataset and investigate:
1. Attack rates by network protocol.
2. Most frequent destination ports in attack traffic.
3. Traffic volume differences between normal and attack records.
4. High-risk network services based on attack rates.
5. Rankings of services associated with attacks.

**Code:** [`Phase4_SQL`](Phase4_SQL)

## 🛠️ Technologies Used

- **Programming Language:** Scala
- **Big Data Framework:** Apache Spark
- **Processing:** Spark DataFrames, RDDs, Spark SQL
- **Data Formats:** CSV, Parquet

## Repository Structure

```text
IT462_BigData/
├── P2_DataPreprocessing.scala    # Data loading and preprocessing
├── Phase3_RDD/                  # Distributed RDD analyses
├── Phase4_SQL                   # Spark SQL queries
└── README.md                    # Project documentation
```

## Getting Started

1. Download the UNSW-NB15 CSV files and the feature-description file from the official dataset page.
2. Set up an Apache Spark environment with Scala support.
3. Update the local dataset paths in the scripts to match your machine.
4. Run the preprocessing code first, followed by the RDD and Spark SQL analyses using the prepared data.

> **Note:** Dataset files and local file paths are environment-specific and may not be included in this repository.

## Team Members

- **Maria Al-Nafisah**
- **Leen Khalid Aldbays**
- **Layan Murayshid**
- **Raghad Omar**
- **Haifa Alsaif**

---

*Developed as an academic group project for IT462 – Big Data at King Saud University.*
