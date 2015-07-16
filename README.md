# Almanac
## Design

Adcade or many other companies the metrics problems they are solving are different, but deep down it's similar. We need a generic solution that can cover problems like this: tracking metrics that is geo, temporal and some other non-numeric dimensions. each dimension have it's own granularity, time can be `SECOND`, `MINUTE`, `HOUR`, `DAY`, etc. geo locations can be grouped in to `GeoRect` that is equivalent to geohashes that the precision is decided by how many digit in the hashes are retained. And the non-numeric dimension, can be anything that is a key-value string pair. GeoTemporal project is certainly much simpler, it only concerns one dimension at a time: time dimension for question 3, geo dimension for question 1 and question 2.

There is quite a few key points we'll tackled here

### Goal 1 easy to record
Goal 1 is to have a simple, hassel-free way to record metrics. This is why people love statsd. But they hate it because it's not feature rich. Dimension and geo are all missing. It's hard to have flexibility and simplicity in the same time. Almanac simply put everything into a single data model called `Metric`. And a collection of Builder api is available for easy metric creation.

### Goal 2 query language
With all these dimensions, data could grow exponentially. Every data-centric application needs to have a easy interface to retrieve data. Almanac is designed with a fluent API similar to SQL syntax and have a plan to make it into a real query language (AQL) in the future. So that you can record and retrieve the metrics in a intuitive way. Please scroll down to API for detail

### Goal 3 streaming
One of the most important goal is to handle thousand of metrics concurrently. For fast message delivery, there is not too much popular options to choose from right now. Kinesis and Kafka is on the top list, while zeromq and the old good socket is the easiest and still fast solution (which requires you to handle a lot of problem yourself) Luckily, Spark Streaming supports them all and more! Since we don't have credential to share for Kinesis, I picked kafka (which I've never used before, it's fun to learn).

### Goal 4 aggregation
Pre-aggregation of data is very necessary, so that they become less in amount, with different granularity. For aggregation, map-reduce is powerful. But spark is even better, which has a much easier interface, in-memo storage, and streaming. So data can be aggregated on the fly even before they been saved to database.

### Goal 5 Storage
How to easily retrieve the data and in short response time is the key of the problem. To archive that, we need to
1. Process as less data as possible.
2. Distributed data for scalability
3. Locally data should be sequential for fast scan.

Distributed Column-based database is the first come to mind, because you can have both distribution and local clustring. There are reasons cassandra are popular. You design the partition key and the clustering key in the way you want, and have a easy query language to interact with. Know how your data is stored so that you can optimize the access. And cassandra has a connector to easy connect with spark. When retieve the preaggregated data, you still need to aggregate facts in real time (because you have no way to know what's the combination of facts to group before query) And keep cassandra and spark partition in sync and aggregate locally before collect to the driver program is a good approach.

### Goal 6 Deployment
For a modern software. Easy deployment is important. Docker and Docker compose does simplify all that!!! In production, I would like to have mesos with docker containers. Spark and Cassandra are all deep integrated with mesos as well. Unfortunately, I did't archieve a "one line starter" at this point given time limitation (the almanac and geotemporal container doesn't play with the dependencies some how).

## Demo

### Prerequisite to run

java > 7, sbt 0.13.8, docker and docker-compose

### Installation and run

in the root folder (which contains both almanac and geotemporal)
``` bash
docker-compose up # only for zookeeper, casssandra, kafka at this time

sbt run # the driver program of spark to control aggregation, and at this moment, it also starts spark locally.
```

## Almanac

almanac is a multi-dimension metrics framework.
It supports StatsD similar interface, pre-aggregate by time and geo location, and provide a SQL like fluent api to query the metrics.
This new project almanac-oss has re-write the original one in akka, spark, cassandra and kafka

### API

currently, this verison of almanac only supports 2 api:

#### record

``` scala
	almanac record Seq(metric1, metric2)
```

sample to make a metric
```scala
import almanac.model.Metric._
metric decrement "some.bucket"
metric withFacts("type" -> "uberx") increment "some.bucket"
metric locate Coordinate(40.72, -73.25) increment("another.bucket", 13)
metric at new DateTime(2015, 4, 15, 12, 00, 00).getTime gauge("bucket.of.gauge", 12)
```

you can also reuse the builder object

```scala
val builder = metric withFacts("type" -> "uberx", "region" -> "us-east")
builder increment "some.bucket"
builder increment "another.bucket"
```

#### query

```scala
	almanac retieve query
```

minimal query

```scala
select("std.impression").query
```

a complete query

```scala
select("std.impression", "std.exit")
    .where( (fact("device") is "iphone")
        and (fact("adId") in ("10001", "10002"))
        and (fact("os") like """*ios*""") )
    .groupBy("adId", "site")
    .orderBy("adId", Order.ASC)
    .locate(GeoRect("dr5ru"), 6)
    .time(DAY, 1420020000000L, 1420106400000L)
    .limit(1000)
    .skip(2000)
    .query
```
note that where, orderby, limit, skip clause of this version of almanac is not implemented.
