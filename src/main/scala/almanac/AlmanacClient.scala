package almanac

import almanac.AlmanacSettings._
import almanac.persist.CassandraMetricRDDRepository
import almanac.spark.SparkMetricsAggregator.AggregationSchedules
import org.apache.spark.{SparkConf, SparkContext}

object AlmanacClient extends App{
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraSeed)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)
    .setAppName("almanac")
    .setMaster(SparkMaster)

  val sc = new SparkContext(conf)

  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)

  val rddRepo = new CassandraMetricRDDRepository(sc, schedules)
}
