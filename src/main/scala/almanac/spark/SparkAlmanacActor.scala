package almanac.spark

import akka.actor._
import almanac.AlmanacSettings
import almanac.api.AlmanacProtocol.{Query, QueryResult, Record}
import almanac.api.MetricSinkFactory
import almanac.cassandra.CassandraMetricRDDRepositoryFactory
import almanac.kafka.KafkaChannelFactory
import almanac.spark.MetricsAggregator._
import almanac.spark.SparkMetricsAggregator._
import org.apache.spark.{Logging, SparkContext}

object SparkAlmanacActor {
  def props(sparkContext: SparkContext): Props = {
    Props(classOf[SparkAlmanacActor], CassandraMetricRDDRepositoryFactory, KafkaChannelFactory, sparkContext)
  }
}

class SparkAlmanacActor(val repoFactory: MetricRDDRepositoryFactory,
                        val sinkFactory: MetricSinkFactory,
                        implicit val sparkContext: SparkContext) extends Actor with Logging with AlmanacSettings {
  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)

  // FIXME: global SparkContext
  implicit val repo = repoFactory.createRepository(schedules)
  val sink = sinkFactory.createSink

  def receive: Receive = {
    case Record(metrics) => {
      log.info(s"Almanac sending $metrics")
      sink.send(metrics)
    }
    case Query(query) =>
      log.info(s"Almanac retrieving $query")
      val resultRDD = repo read query
      /*
      TODO: distinguish no group by or group by no fact
      TODO: add ordering, limit/paging and streaming
       */
      val groupedRDD = if (query.groupNames.isEmpty) resultRDD
                       else resultRDD aggregateMetrics by(query.groupNames)

      sender ! QueryResult(groupedRDD collect())
  }

  override def postStop(): Unit = {
    sparkContext.stop()
    sink.close()
  }
}