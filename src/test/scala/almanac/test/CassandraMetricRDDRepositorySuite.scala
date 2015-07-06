package almanac.test

import almanac.cassandra.CassandraMetricRDDRepository
import almanac.cassandra.CassandraMetricRDDRepository.KeyIndex
import almanac.model.Criteria._
import almanac.model.GeoHash._
import almanac.model.MetricsQuery._
import almanac.model.TimeSpan._
import almanac.model._
import almanac.spark.AggregationSchedules
import almanac.test.TestUtils.{avg, time}
import almanac.util.MD5Helper._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, ConfigMap, FunSuite, Matchers}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class CassandraMetricRDDRepositorySuite extends FunSuite with BeforeAndAfterAll with Matchers {

  // region test data
  val timestamp = new DateTime(2001, 9, 8, 21, 46, 40).getMillis
  val geohash = "dr5ru1pcr6gu"
  val geohash1 = "dr5r"
  val geohash2 = "dr57"
  val facts1 = Map("device" -> "pc", "os" -> "windows")
  val facts2 = Map("device" -> "mobile", "os" -> "osx")

  val metrics = Seq(
    Metric("std.impression", facts1, HOUR, timestamp, geohash1, 62, 824),
    Metric("std.impression", facts2, HOUR, timestamp, geohash2, 62, 824),
    Metric("std.exit", facts1, HOUR, timestamp, geohash2, 62, 824),
    Metric("std.exit", facts2, HOUR, timestamp, geohash1, 62, 824)
  )
  // endregion
  var sc: SparkContext = null
  var ssc: StreamingContext = null
  var schedules: AggregationSchedules = null
  var metricsRdd: RDD[Metric] = null

  override def beforeAll(configMap: ConfigMap) {
    val conf = new SparkConf().setAppName("almanac").setMaster("local[2]")
      .set("spark.cassandra.connection.host", "dev")
    sc = SparkContext getOrCreate conf
    ssc = new StreamingContext(sc, Milliseconds(100))
    schedules = AggregationSchedules(List(4), List(HOUR))
    metricsRdd = sc parallelize metrics
  }

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
    CassandraConnector(sc.getConf) withSessionDo { session =>
      session.execute("use almanac;")
      session.execute("truncate facts;")
    }

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
    CassandraConnector(sc.getConf) withSessionDo { session =>
      session.execute("use almanac;")
      session.execute("truncate facts;")
    }

    repo saveKeys metricsRdd.map(_.key)

    val facts = repo readFacts(Set("std.impression", "std.exit"), GeoFilter("dr5")) collect()
    facts should contain theSameElementsAs Seq(Map("device" -> "pc", "os" -> "windows"),
                                               Map("device" -> "mobile", "os" -> "osx"),
                                               Map("device" -> "mobile", "os" -> "osx"),
                                               Map("device" -> "pc", "os" -> "windows"))
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
    import almanac.spark.MetricsAggregator._
    import almanac.spark.SparkMetricsAggregator._
    val nofactRdd = metricsRdd aggregateMetrics by(Nil)
    repo save nofactRdd
    repo saveKeys nofactRdd.map(_.key)

    val expected = metrics map (_.key) filter (_.bucket=="std.impression") map {k =>
      Metric.Key(k.bucket, Map(), k.span, k.timestamp, k.geohash)
    }

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
    result map(_.key) should contain theSameElementsAs expected
  }

  test ("performance") {
    val repo = new CassandraMetricRDDRepository(sc, schedules)

    def work = (repo read select("std.impression")
      .locate(GeoRect("dr5"), 7)
      .time(HOUR, HOUR(timestamp), HOUR(timestamp) + 3600000)
      .query) collect()

    val num = 10

    def printAvg(allTime: Seq[Long]) = println(s"Elapsed time: ${avg(allTime)} ms")

    printAvg(1 to num map (_=> time { work }))
    printAvg(Await.result( Future.sequence(1 to num map (_=> Future { time { work } } )), 1 hour span))
  }


}
