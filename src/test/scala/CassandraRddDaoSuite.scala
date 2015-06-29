import almanac.model.MetricsQuery._
import almanac.model.TimeSpan.{ALL_TIME, HOUR, RAW}
import almanac.model._
import almanac.persist.{RDDCassandraRepository, RDDCassandraRepository$}
import almanac.spark.SparkMetricsAggregator.AggregationSchedules
import almanac.util.MetricsGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

/**
 * Created by binliu on 6/28/15.
 */
class CassandraRddDaoSuite extends FunSuite with Matchers{

  val conf = new SparkConf().setAppName("almanac")
    .set("spark.cassandra.connection.host", "dev")
  val sc = new SparkContext("local[4]", "test", conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  val schedules = AggregationSchedules(List(4, 0), List(HOUR, ALL_TIME))

  test("test handle") {
    val dao = new RDDCassandraRepository(sc, schedules)
    val q = mutable.Queue[RDD[Metric]]()
    for (i <- 1 to 10) { _: Int =>
      q.enqueue(sc.parallelize(MetricsGenerator.generateRawWithGeo(100)))
    }
    val stream = ssc.queueStream(q, true)
    dao.save(stream)
    //ssc.awaitTermination()
  }

  test("test save") {
    val fromTime = System.currentTimeMillis()
    val metrics = MetricsGenerator.generateRawWithGeo(10)
    val toTime = fromTime + 1000000

    val rdd = sc.parallelize(metrics)
    val dao = new RDDCassandraRepository(sc, schedules)
    dao.save(rdd)
//    dao.read(metrics.head.bucket, metrics.head.geohash, metrics.head.span)
//      .collect().foreach(println)

    val distinctBuckets = metrics.map(_.bucket).toSet.toSeq

    dao.read(
      select(distinctBuckets:_*)
        .locate(12, GeoRect(40.8, 74.0, 40.799, 73.999))
        .time(RAW, fromTime, toTime).query
    ).collect().foreach(println)

//    metrics.foreach { metric =>
//      val inDb = dao.read(metric.bucket, metric.geohash, metric.span)
//      inDb.foreach { m =>
//        println(m)
//      }
//      //println(inDb)
//    }
  }
}
