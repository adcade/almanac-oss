package almanac.test

import almanac.model.Criteria._
import almanac.model.GeoHash._
import almanac.model.MetricsQuery._
import almanac.model.TimeSpan._
import almanac.model._
import almanac.persist.CassandraMetricRDDRepository
import almanac.persist.CassandraMetricRDDRepository.KeyIndex
import almanac.spark.AggregationSchedules
import almanac.test.TestUtils.{avg, time}
import almanac.util.MD5Helper._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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
  val metricsRdd = sc parallelize metrics
  // endregion

  test("test metrics dstream save") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)

    val q = mutable.Queue[RDD[Metric]]()
    val stream = ssc.queueStream(q, oneAtATime = true)
    repo.save(0, HOUR, stream)

    for (_ <- 1 to 3) {
      q += sc makeRDD metrics
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(400)
  }

  test("test facts with geo rdd") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)

    repo saveKeys metricsRdd.map(_.key)

    val result = repo readKeys (Set("std.impression", "std.exit"), GeoFilter(geohash1 ~ 3, 4), nofact) collect()

    result should contain theSameElementsAs Set(
      KeyIndex("std.impression", geohash1, hash(facts1), facts1),
      KeyIndex("std.impression", geohash2, hash(facts2), facts2),
      KeyIndex("std.exit", geohash2, hash(facts1), facts1),
      KeyIndex("std.exit", geohash1, hash(facts2), facts2)
    )
  }

  test("test facts rdd") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)

    repo saveKeys metricsRdd.map(_.key)

    repo readFacts Set("std.impression", "std.exit") collect() foreach println
  }

  test("test metrics query") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)
    repo save metricsRdd
    repo saveKeys metricsRdd.map(_.key)

    val query = select("std.impression")
            .locate(GeoRect("dr5"), 7)
            .time(HOUR, HOUR(timestamp), HOUR(timestamp) + 3600000)
            .query

    val startTime = System.currentTimeMillis
    val result = (repo read query) collect()

    val duration = System.currentTimeMillis - startTime
    duration should be <= 500L
    println(s"Elapsed time: $duration ms")

    result map(_.key) should contain theSameElementsAs (metrics map (_.key) filter (_.bucket=="std.impression"))
  }

  test("test metrics query of no fact metrics") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)
    repo save metricsRdd
    repo saveKeys metricsRdd.map(_.key)

    val expected = metrics map (_.key) filter (_.bucket=="std.impression") map {k =>
      Metric.Key(k.bucket, Map(), k.span, k.timestamp, k.geohash)
    } toSet

    val query = select("std.impression")
      .where(nofact)
      .locate(GeoRect("dr5"), 7)
      .time(HOUR, HOUR(timestamp), HOUR(timestamp) + 3600000)
      .query

    val startTime = System.currentTimeMillis
    val result = (repo read query) collect()
    val duration = System.currentTimeMillis - startTime
    duration should be <= 500L
    println(s"Elapsed time: $duration ms")

    // FIXME: the result seems to be wrong! we didn't save any no fact metrics!!!
    // TODO: this is even slower than the join version
    result map(_.key) should contain theSameElementsAs (expected)
  }

  test ("performance") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)

    def work = (repo read select("std.impression")
      .locate(GeoRect("dr5"), 7)
      .time(HOUR, HOUR(timestamp), HOUR(timestamp) + 3600000)
      .query) collect()

    val num = 10000

    def printAvg(allTime: Seq[Long]) = println(s"Elapsed time: ${avg(allTime)} ms")

    printAvg(Await.result( Future.sequence(1 to num map (_=> Future { time { work } } )), 1 hour))
    printAvg(1 to num map (_=> time { work }))
  }
}
