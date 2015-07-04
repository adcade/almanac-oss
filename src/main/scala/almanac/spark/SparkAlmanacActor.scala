package almanac.spark

import akka.actor._
import almanac.AlmanacSettings._
import almanac.api.AlmanacProtocol.{Query, QueryResult, Record}
import almanac.api.MetricSink
import almanac.spark.MetricsAggregator._
import almanac.spark.SparkMetricsAggregator._
import org.apache.spark.{Logging, SparkContext}

class SparkAlmanacActor(createRepo: AlmanacMetrcRDDRepositoryFactory,
                        sink: MetricSink) extends Actor with Logging {
  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)

  implicit val sc = SparkContext getOrCreate AlmanacSparkConf
  implicit val repo = createRepo(schedules)

  def receive: Receive = {
    case Record(metrics) => sink.send(metrics)
    case Query(query) =>
      // TODO: call aggregator do facts group, ordering and limit/paging
      val resultRDD = repo read query //aggregateMetrics by(query.groupNames)
      sender ! QueryResult(resultRDD collect())
  }

  override def postStop(): Unit = {
    sc.stop()
  }
}