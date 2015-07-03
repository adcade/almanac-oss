package almanac

import almanac.AlmanacSettings._
import almanac.persist.CassandraMetricRDDRepository
import almanac.spark.SparkMetricsAggregator
import almanac.spark.SparkMetricsAggregator.AggregationSchedules
import almanac.util.MetricsReceiver
import org.apache.spark.streaming.{Seconds, Milliseconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

object AlmanacDemo extends App{
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraSeed)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)
    .setAppName("almanac")
    .setMaster(SparkMaster)

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Milliseconds(SparkStreamingBatchDuration))

  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)
  val metricsStream = ssc receiverStream new MetricsReceiver

  metricsStream window(Seconds(10), Seconds(10)) count() print()
  val rddRepo = new CassandraMetricRDDRepository(sc, schedules)
  val aggregator = new SparkMetricsAggregator(metricsStream, rddRepo) schedule schedules

  ssc start()
  ssc awaitTermination()
}
