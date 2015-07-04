package almanac

import java.util.Properties

import _root_.kafka.producer.ProducerConfig
import akka.japi.Util.immutableSeq
import almanac.model.TimeSpan
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf

/* Initializes Akka, Spark, Cassandra and Kafka settings. */
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

  val kafkaConfig = config.getConfig("kafka")

  val KafkaMetricTopic = config.getString("kafka.topic.metric")

  val AlmanacSparkConf = new SparkConf(true)
    .setAppName("almanac")
    .setMaster(SparkMaster)
    .set("spark.cassandra.connection.host", CassandraSeed)
    .set("spark.ui.enabled", config.getBoolean("spark.ui.enabled").toString)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)

  val KafkaConsumerParam = Map[String, String](
    "metadata.broker.list" -> kafkaConfig.getString("metadata.broker.list")
  )

  val KafkaProducerConfig = {
    val properties = new Properties()

    Seq(
      "metadata.broker.list",
      "group.id",
      "zookeeper.connect",
      "key.serializer.class",
      "serializer.class",
      "partitioner.class",
      "request.required.acks"
    ) foreach { key =>
      if (kafkaConfig.hasPath(key)) properties.put(key, kafkaConfig.getString(key))
    }

    new ProducerConfig(properties)
  }
}
