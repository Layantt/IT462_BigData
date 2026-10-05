# Girl 3 - Attack Services Analysis

## Analysis Question

What are the most frequently used services in attack traffic in the final preprocessed dataset?

## RDD Operations Used

Transformations: filter, map, reduceByKey, sortBy

Actions: take, first

## Scala Code

```scala
val df = spark.read.parquet(
  "C:/IT462/preprocessed_dataset.parquet/preprocessed_dataset.parquet"
)

val rdd = df.rdd

// Keep attack records with valid service values only
val validAttackRDD = rdd.filter(row => {
  val service = row.getAs[String]("service")

  row.getAs[String]("Label") == "1" &&
  service != null &&
  service.trim.nonEmpty &&
  service != "-"
})

// Convert each service into (service, 1)
val serviceRDD = validAttackRDD.map(
  row => (row.getAs[String]("service"), 1)
)

// Count the occurrences of each service
val serviceCounts = serviceRDD.reduceByKey(_ + _)

// Sort services by frequency in descending order
val sortedServices = serviceCounts.sortBy(
  x => x._2,
  ascending = false
)

// Get the top 10 most frequent services
val top10Services = sortedServices.take(10)
top10Services.foreach(println)

// Get the most frequent service
val topService = sortedServices.first()

println(
  "Most frequent service in attack traffic: " + topService
)
