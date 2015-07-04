package almanac

import almanac.cassandra.CassandraMetricRDDRepositoryFactory
import almanac.kafka.{KafkaChannelFactory, KafkaChannel}
import almanac.spark.SparkAlmanacEngine

object Almanac extends App {
  val engine = new SparkAlmanacEngine(
    repoFactory = CassandraMetricRDDRepositoryFactory,
    streamFactory = KafkaChannelFactory)
  engine.run()
}
