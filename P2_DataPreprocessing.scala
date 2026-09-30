// ============================================================
// IT462 - Big Data Project - Phase 2
// Dataset: UNSW-NB15
// ============================================================

import org.apache.spark.sql.functions._


// ============================================================
// MEMBER 1 - Data Loading, Profiling, and Integration
// ============================================================


// ------------------------------------------------------------
// 1. File Paths
// ------------------------------------------------------------

val basePath = "C:/IT462/UNSW_NB15"

val file1Path = s"$basePath/UNSW-NB15_1.csv"
val file2Path = s"$basePath/UNSW-NB15_2.csv"
val file3Path = s"$basePath/UNSW-NB15_3.csv"
val file4Path = s"$basePath/UNSW-NB15_4.csv"

val featuresPath = s"$basePath/NUSW-NB15_features.csv"


// ------------------------------------------------------------
// 2. Load Feature Description File
// ------------------------------------------------------------

val featuresDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(featuresPath)

val featuresClean = featuresDF.toDF(
  "No",
  "Name",
  "Type",
  "Description"
)

println("Number of documented features:")
println(featuresClean.count())


// Extract the feature names in their correct order.

val columnNames = featuresClean
  .orderBy("No")
  .select("Name")
  .collect()
  .map(_.getString(0))


// Clean spaces in column names.
// Example: ct_src_ ltm --> ct_src_ltm

val cleanColumnNames =
  columnNames.map(_.trim.replaceAll("\\s+", "_"))

val finalColumnNames =
  cleanColumnNames.map(_.replaceAll("_+", "_"))


// Verify that 49 unique column names remain.

println("Number of column names:")
println(finalColumnNames.length)

println("Number of unique column names:")
println(finalColumnNames.distinct.length)


// ------------------------------------------------------------
// 3. Load the Four Raw UNSW-NB15 Files
// ------------------------------------------------------------

def loadAndName(path: String) = {
  spark.read
    .option("header", "false")
    .csv(path)
    .toDF(finalColumnNames: _*)
}

val file1Named = loadAndName(file1Path)
val file2Named = loadAndName(file2Path)
val file3Named = loadAndName(file3Path)
val file4Named = loadAndName(file4Path)


// ------------------------------------------------------------
// 4. Schema Validation
// ------------------------------------------------------------

println("File 1 columns: " + file1Named.columns.length)
println("File 2 columns: " + file2Named.columns.length)
println("File 3 columns: " + file3Named.columns.length)
println("File 4 columns: " + file4Named.columns.length)

println(
  "File 2 schema matches File 1: " +
  (file2Named.schema == file1Named.schema)
)

println(
  "File 3 schema matches File 1: " +
  (file3Named.schema == file1Named.schema)
)

println(
  "File 4 schema matches File 1: " +
  (file4Named.schema == file1Named.schema)
)

file1Named.printSchema()


// ------------------------------------------------------------
// 5. Initial File Profiling
// ------------------------------------------------------------

val file1Rows = file1Named.count()
val file2Rows = file2Named.count()
val file3Rows = file3Named.count()
val file4Rows = file4Named.count()

println("File 1 rows: " + file1Rows)
println("File 2 rows: " + file2Rows)
println("File 3 rows: " + file3Rows)
println("File 4 rows: " + file4Rows)


// Label = 0 -> Normal
// Label = 1 -> Attack

println("File 1 Label distribution:")
file1Named
  .groupBy("Label")
  .count()
  .orderBy("Label")
  .show()

println("File 2 Label distribution:")
file2Named
  .groupBy("Label")
  .count()
  .orderBy("Label")
  .show()

println("File 3 Label distribution:")
file3Named
  .groupBy("Label")
  .count()
  .orderBy("Label")
  .show()

println("File 4 Label distribution:")
file4Named
  .groupBy("Label")
  .count()
  .orderBy("Label")
  .show()


// ------------------------------------------------------------
// 6. Integrate the Four Files
// ------------------------------------------------------------

val combinedAllDF =
  file1Named
    .unionByName(file2Named)
    .unionByName(file3Named)
    .unionByName(file4Named)


// Verify integrated dataset size.

val combinedRows = combinedAllDF.count()
val combinedColumns = combinedAllDF.columns.length

println("Combined rows: " + combinedRows)
println("Combined columns: " + combinedColumns)


// Target distribution after integration.

println("Combined Label distribution:")

combinedAllDF
  .groupBy("Label")
  .count()
  .orderBy("Label")
  .show()


// Display a small sample.

combinedAllDF
  .select(
    "srcip",
    "sport",
    "dstip",
    "proto",
    "dur",
    "attack_cat",
    "Label"
  )
  .show(10, false)


// ------------------------------------------------------------
// 7. Initial Data-Quality Profiling
// ------------------------------------------------------------

// This section detects issues only.
// No cleaning or removal is performed here.

val nullCounts = combinedAllDF.select(
  combinedAllDF.columns.map { c =>
    sum(
      when(
        col(c).isNull || trim(col(c)) === "",
        1
      ).otherwise(0)
    ).alias(c)
  }: _*
)

println("Missing / empty values per attribute:")
nullCounts.show(false)


// Service distribution.

println("Service distribution:")

combinedAllDF
  .groupBy("service")
  .count()
  .orderBy(desc("count"))
  .show(30, false)


// State distribution.

println("State distribution:")

combinedAllDF
  .groupBy("state")
  .count()
  .orderBy(desc("count"))
  .show(50, false)


// Inspect unusual state value "no".

println("Records where state = no:")

combinedAllDF
  .filter(col("state") === "no")
  .show(20, false)


// Raw attack category distribution.

println("Raw attack category distribution:")

combinedAllDF
  .groupBy("attack_cat")
  .count()
  .orderBy(desc("count"))
  .show(30, false)


// Temporary trim for inspection only.
// combinedAllDF itself is NOT modified.

println("Attack categories after temporary trim for inspection:")

combinedAllDF
  .groupBy(
    trim(col("attack_cat")).alias("attack_cat_trimmed")
  )
  .count()
  .orderBy(desc("count"))
  .show(30, false)


// Check relationship between Label and attack_cat.

println("Label / Attack Category relationship:")

combinedAllDF
  .groupBy("Label", "attack_cat")
  .count()
  .orderBy(col("Label"), desc("count"))
  .show(30, false)


// Protocol distribution.

println("Most common protocols:")

combinedAllDF
  .groupBy("proto")
  .count()
  .orderBy(desc("count"))
  .show(50, false)


// Number of unique protocol categories.

val protocolCount =
  combinedAllDF
    .select("proto")
    .distinct()
    .count()

println("Number of distinct protocols: " + protocolCount)




// ============================================================
// MEMBER 2 - Missing and Invalid Values Cleaning
// ============================================================

// 1. Detect missing values
val missingCounts = df.select(
  df.columns.map(c =>
    sum(
      when(
        col(c).isNull ||
        trim(col(c).cast("string")) === "",
        1L
      ).otherwise(0L)
    ).alias(c)
  ): _*
).first()

df.columns.zipWithIndex.foreach { case (c, i) =>
  val count = missingCounts.getLong(i)

  if (count > 0) {
    println(c + " = " + count)
  }
}


// 2. Check missing attack_cat values by Label
df.groupBy(
  col("Label"),
  (
    col("attack_cat").isNull ||
    trim(col("attack_cat")) === ""
  ).alias("missing_attack_cat")
).count().show(false)


// 3. Clean and standardize attack_cat
val girl2DF = df
  .withColumn(
    "attack_cat",
    when(
      (col("Label") === "0") &&
      (
        col("attack_cat").isNull ||
        trim(col("attack_cat")) === ""
      ),
      lit("Normal")
    ).otherwise(trim(col("attack_cat")))
  )
  .withColumn(
    "attack_cat",
    when(
      col("attack_cat") === "Backdoor",
      lit("Backdoors")
    ).otherwise(col("attack_cat"))
  )


// 4. Keep protocol-specific missing values as NULL
// ct_flw_http_mthd
// is_ftp_login
// ct_ftp_cmd


// 5. Final check
println("Rows before cleaning: " + df.count())
println("Rows after cleaning: " + girl2DF.count())

girl2DF
  .groupBy("attack_cat")
  .count()
  .orderBy(desc("count"))
  .show(false)


// ============================================================
// MEMBER 3 - Duplicates and Outliers
// ============================================================







// ============================================================
// MEMBER 4 - Data Reduction and Feature Selection
// ============================================================







// ============================================================
// MEMBER 5 - Transformation, Feature Engineering & Final Parquet
// ============================================================

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.types._

// 1. Explicit Type Casting (Official UNSW-NB15 Schema)
// Note: If running sequentially from Member 4, use its resulting DataFrame (e.g., girl4_reduced)
val inputDF = spark.read.option("header", "true").csv("girl4_reduced.csv")

val castedDF = inputDF
  .withColumn("dur", col("dur").cast(DoubleType))
  .withColumn("sload", col("sload").cast(DoubleType))
  .withColumn("dload", col("dload").cast(DoubleType))
  .withColumn("sinpkt", col("sinpkt").cast(DoubleType))
  .withColumn("dinpkt", col("dinpkt").cast(DoubleType))
  .withColumn("tcprtt", col("tcprtt").cast(DoubleType))
  .withColumn("synack", col("synack").cast(DoubleType))
  .withColumn("ackdat", col("ackdat").cast(DoubleType))
  .withColumn("spkts", col("spkts").cast(IntegerType))
  .withColumn("dpkts", col("dpkts").cast(IntegerType))
  .withColumn("sport", col("sport").cast(IntegerType))
  .withColumn("dsport", col("dsport").cast(IntegerType))
  .withColumn("sttl", col("sttl").cast(IntegerType))
  .withColumn("dttl", col("dttl").cast(IntegerType))
  .withColumn("sloss", col("sloss").cast(IntegerType))
  .withColumn("dloss", col("dloss").cast(IntegerType))
  .withColumn("sbytes", col("sbytes").cast(LongType))
  .withColumn("dbytes", col("dbytes").cast(LongType))
  .withColumn("res_bdy_len", col("res_bdy_len").cast(LongType))
  .withColumn("ct_flw_http_mthd", col("ct_flw_http_mthd").cast(IntegerType))
  .withColumn("is_ftp_login", col("is_ftp_login").cast(IntegerType))
  .withColumn("ct_ftp_cmd", col("ct_ftp_cmd").cast(IntegerType))
  .withColumn("Stime", col("Stime").cast(LongType).cast(TimestampType))
  .withColumn("Ltime", col("Ltime").cast(LongType).cast(TimestampType))
  .withColumn("is_sm_ips_ports", col("is_sm_ips_ports").cast(IntegerType))
  .withColumn("Label", col("Label").cast(IntegerType))

// 2. Feature Engineering
val engineeredDF = castedDF
  .withColumn("total_bytes", col("sbytes") + col("dbytes"))
  .withColumn("total_pkts", col("spkts") + col("dpkts"))
  .withColumn("byte_ratio", col("sbytes") / (col("total_bytes") + 1.0))

// 3. Preparatory Categorical Encoding (StringIndexer)
val indexerProto = new StringIndexer().setInputCol("proto").setOutputCol("proto_indexed").setHandleInvalid("keep")
val indexerService = new StringIndexer().setInputCol("service").setOutputCol("service_indexed").setHandleInvalid("keep")
val indexerState = new StringIndexer().setInputCol("state").setOutputCol("state_indexed").setHandleInvalid("keep")

val indexedDF = indexerState.fit(
  indexerService.fit(
    indexerProto.fit(engineeredDF).transform(engineeredDF)
  ).transform(engineeredDF)
).transform(engineeredDF)

val finalDF = indexedDF

// 4. Output Snapshot (15 Rows)
finalDF.select("proto", "service", "state", "dur", "sbytes", "dbytes", "total_bytes", "total_pkts", "byte_ratio", "Label").show(15, false)

// 5. Save Final Preprocessed Parquet Dataset
finalDF.write.mode("overwrite").parquet("preprocessed_dataset.parquet")
