package almanac

import akka.japi.Util.immutableSeq
import almanac.model.TimeSpan
import com.typesafe.config.ConfigFactory

/* Initializes Akka, Cassandra and Spark settings. */
object AlmanacSettings {
  val rootConfig = ConfigFactory.load

  protected val config = rootConfig.getConfig("almanac")

  val SparkMaster: String = config.getString("spark.master")

  val SparkCleanerTtl: Int = config.getInt("spark.cleaner.ttl")

  val SparkStreamingBatchDuration: Long = config.getLong("spark.streaming.batch.duration")

  val TimeSchedules = immutableSeq(config.getStringList("aggregation.schedule.time")) map (TimeSpan(_)) toList

  val GeoSchedules = immutableSeq(config.getIntList("aggregation.schedule.geo")) map(_.intValue) toList

  val CassandraSeed: String = config.getString("cassandra.connection.host")

  val CassandraKeyspace = config.getString("cassandra.keyspace")

  val CassandraMetricsTable = config.getString("cassandra.table.metrics")

  val CassandraFactsTable = config.getString("cassandra.table.facts")


}
