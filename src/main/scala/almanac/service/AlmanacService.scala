package almanac.service

import akka.actor.ActorRef
import akka.util.Timeout
import almanac.model.{Criteria, Metric, MetricsQuery}
import almanac.persist.MetricRDDRepository
import almanac.service.MetricsProtocol.Record

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.concurrent.duration._

trait AlmanacService {
  def createSpace(space: String): Unit

  def query(query: MetricsQuery): Future[Seq[Metric]]

  def buckets(criteria: Criteria): Future[Seq[String]]

  def buckets(criteria: Criteria, pattern: String): Future[Seq[String]]

  def distinctValues(fact: String, bucket: String): Future[Seq[String]]

  def record(metrics: Metric*): Unit
}

class ActorAlmanacService(alamancActor: ActorRef, repo: MetricRDDRepository) extends AlmanacService {
  implicit val timeout = Timeout(100 millis)

  override def createSpace(space: String): Unit = ???

  override def record(metrics: Metric*): Unit = alamancActor ! Record(metrics)

  override def distinctValues(fact: String, bucket: String): Future[Seq[String]] = ???

  override def buckets(criteria: Criteria): Future[Seq[String]] = ???

  override def buckets(criteria: Criteria, pattern: String): Future[Seq[String]] = ???

  override def query(query: MetricsQuery): Future[Seq[Metric]] = Future {repo read query} map (_.collect())
}