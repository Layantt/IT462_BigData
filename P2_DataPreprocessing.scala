// ============================================================
// IT462 - Big Data Project - Phase 2
// Dataset: UNSW-NB15
// ============================================================

import org.apache.spark.sql.functions._


// ============================================================
// MEMBER 1 - Data Loading, Profiling, and Integration
// ============================================================







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
// MEMBER 5 - Modeling and Evaluation
// ============================================================
