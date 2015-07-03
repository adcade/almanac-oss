package almanac.service

import akka.actor.ActorSelection
import akka.pattern._
import almanac.model.{Criteria, Metric, MetricsQuery}
import almanac.service.MetricsProtocol.{Query, QueryResult, Record}
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class ActorAlmanacClient(almanacActor: ActorSelection) extends AlmanacService {
  implicit val timeout = Timeout(10 seconds)

  override def createSpace(space: String): Unit = ???

  override def record(metrics: Metric*): Unit = almanacActor ! Record(metrics)

  override def distinctValues(fact: String, bucket: String, geohash: String = ""): Future[Seq[String]] = ???

  override def buckets(criteria: Criteria): Future[Seq[String]] = ???

  override def buckets(criteria: Criteria, pattern: String): Future[Seq[String]] = ???

  override def query(query: MetricsQuery): Future[Seq[Metric]] =
    (almanacActor ? Query(query)) map { case QueryResult(metrics) => metrics}
}
