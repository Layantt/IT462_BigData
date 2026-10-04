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

##Sample Results
Number of distinct protocols: 135

Top 10 protocols:
- tcp: 1,448,858
- udp: 588,026
- arp: 6,658
- unas: 4,765
- ospf: 3,964
- icmp: 498
- sctp: 444
- any: 138
- gre: 95
- rsvp: 92

##Insight
The final preprocessed dataset contains 135 distinct network protocols. TCP and UDP are the most frequently occurring protocols, accounting for the vast majority of the network traffic.
