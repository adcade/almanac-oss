package almanac.api

import almanac.model.{Criteria, Metric, MetricsQuery}

import scala.concurrent.Future

trait AlmanacService {
  def createSpace(space: String): Unit

  def retrieve(query: MetricsQuery): Future[Seq[Metric]]

  def stream(query: MetricsQuery, batch: Int): Stream[Seq[Metric]] = ???

  def buckets(criteria: Criteria): Future[Seq[String]]

  def buckets(criteria: Criteria, pattern: String): Future[Seq[String]]

  def distinctValues(fact: String, bucket: String, geohash: String = ""): Future[Seq[String]]

  def record(metrics: Metric*): Unit
}
