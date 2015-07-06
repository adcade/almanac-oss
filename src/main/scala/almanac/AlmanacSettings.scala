package almanac

import java.util.Properties

import akka.japi.Util.immutableSeq
import almanac.model.TimeSpan
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkConf}

/* Initializes Akka, Spark, Cassandra and Kafka settings. */
object AlmanacSettings {
  val rootConfig = ConfigFactory.load

  protected val config = rootConfig.getConfig("almanac")

  val SparkStreamingBatchDuration: Long = config.getLong("spark.streaming.batch.duration")

  val TimeSchedules = immutableSeq(config.getStringList("aggregation.schedule.time")).map(TimeSpan(_)).toList

  val GeoSchedules = immutableSeq(config.getIntList("aggregation.schedule.geo")).map(_.intValue).toList

  val CassandraKeyspace = config.getString("cassandra.keyspace")

  val CassandraCreationScriptPath = config.getString("cassandra.script.creation.path")

  val CassandraMetricsTable = config.getString("cassandra.table.metrics")

  val CassandraFactsTable = config.getString("cassandra.table.facts")

  val kafkaConfig = config.getConfig("kafka")

  val KafkaMetricTopic = config.getString("kafka.topic.metric.name")
  val KafkaMetricTopicPartitionNum = config.getInt("kafka.topic.metric.partition.num")
  val KafkaMetricTopicReplicationFactor = config.getInt("kafka.topic.metric.replication.factor")

  val AlmanacSparkConf = new SparkConf(true)
    .setAppName("almanac")
    .setMaster(config.getString("spark.master"))
    .set("spark.cassandra.connection.host", config.getString("cassandra.connection.host"))
    .set("spark.cleaner.ttl", config.getInt("spark.cleaner.ttl").toString)
    .set("spark.ui.enabled", config.getBoolean("spark.ui.enabled").toString)

  val AlmanacGlobalSparkContext = SparkContext getOrCreate AlmanacSparkConf

  val KafkaConsumerParam = Map[String, String](
    "metadata.broker.list" -> kafkaConfig.getString("metadata.broker.list")
  )

  val KafkaProducerProperties = {
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

    properties
  }
}
