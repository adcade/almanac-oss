package almanac

import almanac.AlmanacSettings._
import almanac.persist.CassandraMetricRDDRepository
import almanac.spark.AggregationSchedules
import almanac.spark.SparkMetricsAggregator._
import almanac.util.RandomMetricsReceiver
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object AlmanacDemo extends App{
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraSeed)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)
    .setAppName("almanac")
    .setMaster(SparkMaster)

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Milliseconds(SparkStreamingBatchDuration))

  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)
  val metricsStream = ssc receiverStream new RandomMetricsReceiver

  implicit val rddRepo = new CassandraMetricRDDRepository(sc, schedules)
  metricsStream aggregateWithSchedule schedules
  metricsStream window(Seconds(10), Seconds(10)) count() print()

  ssc start()
  ssc awaitTermination()
}
