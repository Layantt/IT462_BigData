# Girl 2 - Protocol Analysis

## Analysis Question
What are the most frequently used network protocols in the final preprocessed dataset, and how diverse are the protocols?

## RDD Operations Used
Transformations: map, distinct, reduceByKey, sortBy
Actions: count, take

## Scala Code

```scala
val protoRDD = finalDF
  .select("proto")
  .rdd
  .map(row => row.getString(0))

val distinctProtoRDD = protoRDD.distinct()

distinctProtoRDD.count()

val protocolCountsRDD =
  protoRDD
    .map(proto => (proto, 1))
    .reduceByKey(_ + _)

val sortedProtocolRDD =
  protocolCountsRDD
    .sortBy(_._2, ascending = false)

sortedProtocolRDD.take(10)
