package almanac.test

import almanac.model.MetricsQuery._
import almanac.model.TimeSpan._
import almanac.model._
import almanac.persist.CassandraMetricRDDRepository
import almanac.spark.SparkMetricsAggregator
import almanac.spark.SparkMetricsAggregator.AggregationSchedules
import almanac.util.MetricsGenerator
import almanac.util.MetricsGenerator._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

class CassandraMetricRDDRepositorySuite extends FunSuite with Matchers{

  val conf = new SparkConf().setAppName("almanac")
    .set("spark.cassandra.connection.host", "dev")
  val sc = new SparkContext("local[4]", "test", conf)
  val ssc = new StreamingContext(sc, Milliseconds(100))
  val schedules = AggregationSchedules(List(4), List(HOUR))

  test("test metrics dstream save") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)

    val q = mutable.Queue[RDD[Metric]]()
    val stream = ssc.queueStream(q, true)
    repo.save(0, RAW, stream)

    for (_ <- 1 to 3) {
      q += sc.makeRDD((1 to 10) map (_ => generateRawWithGeo))
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(400)

  }

  test("test metrics rdd save") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)
    repo save sc.parallelize((1 to 10) map (_ => generateRawWithGeo))
  }

  test("test facts with geo rdd") {
    import SparkMetricsAggregator._
    val repo = new CassandraMetricRDDRepository(sc, schedules)
    val metrics = (1 to 10) map (_ => generateRawWithGeo)
    val rdd = sc.makeRDD(metrics).aggregateByGeoPrecision(4)
    repo.saveFacts(rdd)
    val distinctBuckets = metrics.map(_.bucket).toSet
    val distinctGeohashes = GeoRect(latRange, lngRange).geohashes(4)
    println(distinctGeohashes)
    repo.readFacts(distinctBuckets, distinctGeohashes).collect().foreach(println)
  }

  test("test facts rdd") {
    import SparkMetricsAggregator._
    val repo = new CassandraMetricRDDRepository(sc, schedules)
    val metrics = (1 to 10) map (_ => generateRawWithFacts)
    val rdd = sc.makeRDD(metrics).aggregateByGeoPrecision(4)
    repo.saveFacts(rdd)

    val distinctBuckets = metrics.map(_.bucket).toSet
    repo.readFacts(distinctBuckets, Set("")).collect().foreach(println)
  }

  test("test metrics query") {
    val fromTime = System.currentTimeMillis()
//    // make stream
//    val q = mutable.Queue[RDD[Metric]]()
//    val metrics = (1 to 10) map (_ => generateRawWithGeo)
//    q += sc.makeRDD(metrics)
//    val stream = ssc.queueStream(q, true)
//
//    // save with almanac aggregator
    val repo = new CassandraMetricRDDRepository(sc, schedules)
//    SparkMetricsAggregator(stream, dao).schedule(schedules)
//    ssc.start()
//    ssc.awaitTerminationOrTimeout(300)

//    val distinctBuckets = metrics.map(_.bucket).toSet.toSeq

    val query = select(MetricsGenerator.buckets:_*)
            .locate(4, GeoRect(latRange, lngRange))
            .time(HOUR, fromTime - 7200000, fromTime + 7200000).query

    repo.read(query).foreach(println)
  }
}
