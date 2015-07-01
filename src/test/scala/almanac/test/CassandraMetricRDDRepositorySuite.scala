package almanac.test

import almanac.model.GeoHash._
import almanac.model.MetricsQuery._
import almanac.model.TimeSpan._
import almanac.model._
import almanac.persist.CassandraMetricRDDRepository
import almanac.persist.CassandraMetricRDDRepository.FactIndex
import almanac.spark.SparkMetricsAggregator._
import almanac.util.MD5Helper._
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

  // region test data
  val timestamp = new DateTime(2001, 9, 8, 21, 46, 40).getMillis

  val geohash = "dr5ru1pcr6gu"
  val geohash1 = "dr5r"
  val geohash2 = "dr57"
  val facts1 = Map("device"->"pc", "os"->"windows")
  val facts2 = Map("device"->"mobile", "os"->"osx")

  val metrics = Seq(
    Metric("std.impression", facts1, HOUR, timestamp, geohash1, 62, 824),
    Metric("std.impression", facts2, HOUR, timestamp, geohash2, 62, 824),
    Metric("std.exit", facts1, HOUR, timestamp, geohash2, 62, 824),
    Metric("std.exit", facts2, HOUR, timestamp, geohash1, 62, 824)
  )
  val metricsRdd = sc.parallelize(metrics)
  // endregion

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

    repo saveFacts metricsRdd

    val result = repo readFacts (Set("std.impression", "std.exit"), GeoFilter(geohash1 ~ 3, 4)) collect()

    result should contain theSameElementsAs(Set(
      FactIndex("std.impression", geohash1, hash(facts1), facts1),
      FactIndex("std.impression", geohash2, hash(facts2), facts2),
      FactIndex("std.exit",       geohash2, hash(facts1), facts1),
      FactIndex("std.exit",       geohash1, hash(facts2), facts2)
    ))
  }

  test("test facts rdd") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)

    repo saveFacts metricsRdd

    repo readFacts (Set("std.impression", "std.exit")) collect() foreach println
  }

  test("test metrics query") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)

    repo save metricsRdd
    repo saveFacts metricsRdd

//    val startTime = System.currentTimeMillis
    val query = select("std.impression")
            .locate(GeoRect("dr5"), 7)
            .time(HOUR, HOUR(timestamp), HOUR(timestamp) + 3600000)
            .query

    val result = (repo read query) collect()

//    val duration = System.currentTimeMillis - startTime
//    duration should be <= (200L)

//    result.map(_.key) should contain theSameElementsAs(
//      metrics.map(_.key).filter(_.bucket == "std.impression"))

    result foreach println
  }
}
