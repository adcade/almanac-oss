package almanac

import almanac.AlmanacSettings._
import almanac.cassandra.CassandraMetricRDDRepositoryFactory
import almanac.kafka.KafkaChannelFactory
import almanac.spark.SparkAlmanacEngine

object Almanac extends App {
  val sc =  AlmanacGlobalSparkContext

  CassandraMetricRDDRepositoryFactory.createTable(sc.getConf)
  KafkaChannelFactory.createTopicIfNotExists(KafkaMetricTopic, KafkaMetricTopicPartitionNum, KafkaMetricTopicReplicationFactor)

  val engine = new SparkAlmanacEngine(
    repoFactory = CassandraMetricRDDRepositoryFactory,
    streamFactory = KafkaChannelFactory,
    sparkContext = sc)
  engine.run()
}
