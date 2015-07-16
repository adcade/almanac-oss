package almanac

import java.util.Properties

import akka.japi.Util.immutableSeq
import almanac.model.{Metric, TimeSpan}
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkConf}

/**
 *  Initializes Akka, Spark, Cassandra and Kafka settings
 */
trait AlmanacSettings {
  val rootConfig = ConfigFactory.load

  protected val config = rootConfig.getConfig("almanac")

  lazy val SparkStreamingBatchDuration: Long = config.getLong("spark.streaming.batch.duration")

  lazy val TimeSchedules = immutableSeq(config.getStringList("aggregation.schedule.time")).map(TimeSpan(_)).toList

  lazy val GeoSchedules = immutableSeq(config.getIntList("aggregation.schedule.geo")).map(_.intValue).toList

  lazy val CassandraKeyspace = config.getString("cassandra.keyspace")

  lazy val CassandraCreationScriptPath = config.getString("cassandra.script.creation.path")

  lazy val CassandraMetricsTable = config.getString("cassandra.table.metrics")

  lazy val CassandraFactsTable = config.getString("cassandra.table.facts")

  lazy val kafkaConfig = config.getConfig("kafka")

  lazy val KafkaMetricTopic = config.getString("kafka.topic.metric.name")

  lazy val KafkaMetricTopicPartitionNum = config.getInt("kafka.topic.metric.partition.num")

  lazy val KafkaMetricTopicReplicationFactor = config.getInt("kafka.topic.metric.replication.factor")

  lazy val AlmanacSparkConf = new SparkConf(true)
    .setAppName("almanac")
    .setMaster(config.getString("spark.master"))
    .set("spark.cassandra.connection.host", config.getString("cassandra.connection.host"))
    .set("spark.cleaner.ttl", config.getInt("spark.cleaner.ttl").toString)
    .set("spark.ui.enabled", config.getBoolean("spark.ui.enabled").toString)
    .set("spark.serializer", config.getString("spark.serializer"))
    .registerKryoClasses(Array(classOf[Metric],
                               classOf[Metric.Key],
                               classOf[Metric.Value]))

  lazy val AlmanacGlobalSparkContext = SparkContext getOrCreate AlmanacSparkConf

  lazy val KafkaConsumerParam = Map[String, String](
    "metadata.broker.list" -> kafkaConfig.getString("metadata.broker.list")
  )

  lazy val KafkaProducerProperties = {
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
