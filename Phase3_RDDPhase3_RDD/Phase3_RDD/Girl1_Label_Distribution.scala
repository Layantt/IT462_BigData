
// Girl 1 - Label Distribution

val df = spark.read.parquet(
  "C:/Users/asanv/Downloads/last semester/Big data system 462/Project/BigData Project/preprocessed_dataset.parquet"
)

val rdd = df.rdd

val totalRecords = rdd.count()

val labelRDD = rdd.map(row => (row.getAs[String]("Label").toInt, 1))

val labelCounts = labelRDD.reduceByKey(_ + _)

val distribution = labelCounts.collect()

println("Total records: " + totalRecords)
distribution.foreach(println)
