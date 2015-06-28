package almanac.persist

import akka.actor.Actor
import almanac.model.{Metric, MetricsQuery}
import almanac.service.MetricsProtocol.Query
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.concurrent.Future

trait MetricsRepository {
  def save(metrics: Seq[Metric]): Future[Unit]

  def load(query: MetricsQuery): Future[Seq[Metric]]
}

trait MetricDStreamPersistor {
  def save(stream: RDD[Metric]): Future[Unit]
}

case class Persist(stream: DStream[Metric])

class InMemoMetricsRepositoryActor extends Actor with MetricsRepository{
  val store = Map[String, mutable.Buffer[Metric]]().withDefaultValue(mutable.Buffer())

  implicit val ec = context.dispatcher

  def receive = {
    case Persist(stream) => {
      var futures = mutable.Buffer[Future[Unit]]()
      stream foreachRDD {
        rdd => futures += save(rdd.collect())
      }
      Future.reduce(futures) {(r,_) => r}
    }
    case Query(query) => {
      load(query)
    }
  }

  override def save(metrics: Seq[Metric]): Future[Unit] = {
    Future {
      metrics foreach { m =>
        val key: String = m.bucket + m.geohash + m.span + m.timestamp
        store(key) += m
      }
    }
  }

  override def load(query: MetricsQuery): Future[Seq[Metric]] = {
    val gf = query.geoFilter
    val tf = query.timeFilter
    Future {
      (for { bucket <- query.buckets
             geohash <- gf.rect.geohashes(gf.precision)
//             timestamp <- tf.fromTime to tf.toTime by tf.span.millisec
             metric <- store(bucket + geohash + tf.span + tf.fromTime)
      } yield metric).toSeq
    }
  }
}