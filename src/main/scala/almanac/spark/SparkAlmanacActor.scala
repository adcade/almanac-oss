package almanac.spark

import akka.actor._
import almanac.AlmanacSettings._
import almanac.api.AlmanacProtocol.{Query, QueryResult, Record}
import almanac.spark.SparkMetricsAggregator._
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext}

class SparkClientActor(createRepo: AlmanacMetrcRDDRepositoryFactory,
                       channel: SparkMetricChannel) extends Actor with Logging {
  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)

  implicit val sc = SparkContext getOrCreate AlmanacSparkConf
  implicit val repo = createRepo(schedules)

  def receive: Receive = {
    case Record(metrics) => channel.send(metrics)
    case Query(query) =>
      println("query")
      // TODO: call aggregator do facts group, ordering and limit/paging
      val resultRDD = repo read query aggregateByFacts query.groupNames
      sender ! QueryResult(resultRDD collect())
  }
}