package almanac.test

import almanac.model.MetricsQuery._
import almanac.model.TimeSpan._
import almanac.model._
import almanac.persist.CassandraMetricRDDRepository
import almanac.spark.SparkMetricsAggregator._
import almanac.util.MetricsGenerator._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
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
    val repo = new CassandraMetricRDDRepository(sc, schedules)
    val metrics = (1 to 10) map (_ => generateRawWithGeo)
    val rdd = sc.makeRDD(metrics).aggregateByGeoPrecision(4)

    repo.saveFacts(rdd)

    val distinctBuckets = metrics.map(_.bucket).toSet
    val distinctGeohashes = GeoRect(latRange, lngRange).geohashes(Set(4))

    repo.readFacts(distinctBuckets, GeoFilter(4, GeoRect(latRange, lngRange))).collect().foreach(println)
  }

  test("test facts rdd") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)
    val metrics = (1 to 10) map (_ => generateRawWithFacts)
    val rdd = sc.makeRDD(metrics).aggregateByGeoPrecision(4)

    repo.saveFacts(rdd)

    val distinctBuckets = metrics.map(_.bucket).toSet

    repo.readFacts(distinctBuckets, GeoFilter.WORLDWIDE).collect().foreach(println)
  }

  test("test metrics query") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)

    val timestamp = new DateTime(2001, 9, 8, 21, 46, 40).getMillis

    val geohash = "dr5ru1pcr6gu"
    val geohash1 = "dr5ru1p"
    val geohash2 = "dr5ru1q"
    val facts1 = Map("device"->"pc", "os"->"windows")
    val facts2 = Map("device"->"mobile", "os"->"osx")

    val metrics = Seq(
      Metric("impression", facts1, HOUR, timestamp, geohash1, 62, 824),
      Metric("impression", facts2, HOUR, timestamp, geohash2, 62, 824),
      Metric("exit", facts1, HOUR, timestamp, geohash2, 62, 824),
      Metric("exit", facts2, HOUR, timestamp, geohash1, 62, 824)
    )
    val rdd = sc.parallelize(metrics)
    repo save rdd
    repo saveFacts rdd

    println("saved")

    println(new DateTime(HOUR(timestamp)), new DateTime(HOUR(timestamp) + 3600000))

    val query = select("impression", "exit")
            .locate(7, GeoRect("dr5ru1"))
            .time(HOUR, HOUR(timestamp) - 36000000, HOUR(timestamp) + 36000000)
            .query

    repo read query foreach println

    println("read")
  }
}
