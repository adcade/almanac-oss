package almanac.spark

import akka.actor.{Actor, ActorRef}
import akka.pattern._
import akka.util.Timeout
import almanac.model.{Metric, TimeSpan}
import almanac.service.MetricsProtocol.{Query, Record}
import almanac.spark.SparkMetricsAggregator._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration._

case class MetricsAggregation(timeSchedule: Seq[TimeSpan], geoSchedule: Seq[Int])
case class MetricsRDD(rdd: RDD[Metric])
case class MetricsDStream(strea: DStream[Metric])

class SparkAlmanacActor(persistor: ActorRef) extends Actor {

  implicit val timeout = Timeout(100 millis)
  implicit val ec = context.dispatcher

  def receive: Receive = {
    case Record(metrics) => {

    }
    case q @ Query(query) => {
      val future = (persistor ? q).collect ({
        case MetricsRDD(rdd) => rdd aggregateByFacts (query.groupNames:_*)
      })
      future.pipeTo(sender)
    }
  }
}
