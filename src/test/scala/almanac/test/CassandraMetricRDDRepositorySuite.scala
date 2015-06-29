package almanac.test

import almanac.model.MetricsQuery._
import almanac.model.TimeSpan._
import almanac.model._
import almanac.persist.CassandraMetricRDDRepository
import almanac.persist.CassandraMetricRDDRepository._
import almanac.spark.SparkMetricsAggregator
import almanac.spark.SparkMetricsAggregator.AggregationSchedules
import almanac.util.MetricsGenerator
import com.datastax.spark.connector._
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
    val dao = new CassandraMetricRDDRepository(sc, schedules)

    val q = mutable.Queue[RDD[Metric]]()
    val stream = ssc.queueStream(q, true)
    dao.save(0, RAW, stream)

    for (_ <- 1 to 3) {
      q += sc.makeRDD(MetricsGenerator.generateRawWithGeo(10))
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(400)

  }

  test("test metrics rdd save") {
    val dao = new CassandraMetricRDDRepository(sc, schedules)
    dao.save(sc.parallelize(MetricsGenerator.generateRawWithGeo(10)))
  }

  test("test facts with geo rdd") {
    import SparkMetricsAggregator._
    val dao = new CassandraMetricRDDRepository(sc, schedules)
    val metrics = MetricsGenerator.generateRawWithGeo(10)
    val rdd = sc.makeRDD(metrics).aggregateByGeoPrecision(4)
    dao.saveFacts(rdd)
    val distinctBuckets = metrics.map(_.bucket).toSet
    val distinctGeohashes = GeoRect(MetricsGenerator.latRange, MetricsGenerator.lngRange).geohashes(4)
    println(distinctGeohashes)
    dao.readFacts(distinctBuckets, distinctGeohashes).collect().foreach(println)
  }

  test("test facts rdd") {
    import SparkMetricsAggregator._
    val dao = new CassandraMetricRDDRepository(sc, schedules)
    val metrics = MetricsGenerator.generateRawWithFacts(10)
    val rdd = sc.makeRDD(metrics).aggregateByGeoPrecision(4)
    dao.saveFacts(rdd)

    val distinctBuckets = metrics.map(_.bucket).toSet
    dao.readFacts(distinctBuckets, Set("")).collect().foreach(println)
  }

  test("test metrics query") {
    val fromTime = System.currentTimeMillis()
    // make stream
    val q = mutable.Queue[RDD[Metric]]()
    val metrics = MetricsGenerator.generateRawWithGeo(10)
    q += sc.makeRDD(metrics)
    val stream = ssc.queueStream(q, true)

    // save with almanac aggregator
    val dao = new CassandraMetricRDDRepository(sc, schedules)
    SparkMetricsAggregator(stream, dao).schedule(schedules)
    ssc.start()
    ssc.awaitTerminationOrTimeout(150)

    val distinctBuckets = metrics.map(_.bucket).toSet.toSeq

//    dao.read(
//      select(distinctBuckets:_*)
//        .locate(4, GeoRect(MetricsGenerator.latRange, MetricsGenerator.lngRange))
//        .time(HOUR, fromTime - 3600000, fromTime + 3600000).query
//    ).collect().foreach(println)

    val query = select(distinctBuckets:_*)
            .locate(4, GeoRect(MetricsGenerator.latRange, MetricsGenerator.lngRange))
            .time(HOUR, fromTime - 3600000, fromTime + 3600000).query

    dao.readFacts(query.buckets, query.geoFilter.rect.geohashes(query.geoFilter.precision))
      .joinWithCassandraTable(KEYSPACE, METRICS_TABLE)
      .select(
        COLUMN_NAMES.BUCKET,
        COLUMN_NAMES.GEOHASH,
        COLUMN_NAMES.SPAN,
        COLUMN_NAMES.TIMESTAMP,
        COLUMN_NAMES.COUNT,
        COLUMN_NAMES.TOTAL)
      .where(toTimeConditions(query.timeFilter))

  }

  private def toTimeConditions(filter: TimeFilter): String = {
    ( if (filter == ALL_TIME) Seq(
      s"${COLUMN_NAMES.TIMESTAMP} = 0"                    // timestamp = 0
    ) else Seq(
      s"${COLUMN_NAMES.TIMESTAMP} >= ${filter.fromTime}", // timestamp >= $fromTime
      s"${COLUMN_NAMES.TIMESTAMP} < ${filter.toTime}"     // timestamp < $toTime
    ) :+ s"${COLUMN_NAMES.SPAN} = ${filter.span.index}"   // span = $span.index
      ) mkString " and "
  }
}
