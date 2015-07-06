package almanac.spark

import akka.actor._
import almanac.AlmanacSettings._
import almanac.api.AlmanacProtocol.{Query, QueryResult, Record}
import almanac.api.{MetricSinkFactory, MetricSink}
import almanac.cassandra.CassandraMetricRDDRepositoryFactory
import almanac.kafka.KafkaChannelFactory
import almanac.spark.MetricsAggregator._
import almanac.spark.SparkMetricsAggregator._
import org.apache.spark
import org.apache.spark.metrics.sink
import org.apache.spark.{Logging, SparkContext}

object SparkAlmanacActor {
  def props: Props = {
    Props(classOf[SparkAlmanacActor], CassandraMetricRDDRepositoryFactory, KafkaChannelFactory)
  }
}

class SparkAlmanacActor(repoFactory: AlmanacMetrcRDDRepositoryFactory,
                        sinkFactory: MetricSinkFactory) extends Actor with Logging {
  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)

  // FIXME: global SparkContext
  implicit val sc = AlmanacGlobalSparkContext
  implicit val repo = repoFactory.createRepository(schedules)
  val sink = sinkFactory.createSink

  def receive: Receive = {
    case Record(metrics) => sink.send(metrics)
    case Query(query) =>
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
    sc.stop()
    sink.close()
  }
}