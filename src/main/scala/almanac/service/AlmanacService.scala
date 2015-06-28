package almanac.service

import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import almanac.model.{Criteria, Metric, MetricsQuery}
import almanac.service.MetricsProtocol.{Query, QueryResult, Record}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

trait AlmanacService {
  def createSpace(space: String): Unit

  def query(query: MetricsQuery): Future[Seq[Metric]]

  def buckets(criteria: Criteria): Future[Seq[String]]

  def buckets(criteria: Criteria, pattern: String): Future[Seq[String]]

  def distinctValues(fact: String, bucket: String): Future[Seq[String]]

  def record(metrics: Metric*): Unit
}

class ActorAlmanacService(alamancActor: ActorRef)(implicit ec: ExecutionContext) extends AlmanacService {
  implicit val timeout = Timeout(100 millis)

  override def createSpace(space: String): Unit = ???

  override def record(metrics: Metric*): Unit = alamancActor ! Record(metrics)

  override def distinctValues(fact: String, bucket: String): Future[Seq[String]] = ???

  override def buckets(criteria: Criteria): Future[Seq[String]] = ???

  override def buckets(criteria: Criteria, pattern: String): Future[Seq[String]] = ???

  override def query(query: MetricsQuery): Future[Seq[Metric]] = (alamancActor ? Query(query)).map {
    case QueryResult(metrics) => metrics
  }
}