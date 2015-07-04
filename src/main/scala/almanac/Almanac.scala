package almanac

import almanac.cassandra.CassandraMetricRDDRepositoryFactory
import almanac.kafka.KafkaChannel
import almanac.spark.SparkAlmanacEngine

object Almanac extends App {
  val engine = new SparkAlmanacEngine(CassandraMetricRDDRepositoryFactory, KafkaChannel)
  engine.start()
}
