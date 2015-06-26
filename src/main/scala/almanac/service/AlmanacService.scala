package almanac.service

import almanac.model.{Criteria, Metric, MetricsQuery}

import scala.concurrent.Future

trait AlmanacService {
  def query(query: MetricsQuery): Future[Seq[Metric]]

  def buckets(criteria: Criteria): Future[Seq[String]]

  def buckets(criteria: Criteria, pattern: String): Future[Seq[String]]

  def distinctValues(fact: String, bucket: String): Future[Seq[String]]

  def record(metric: Metric*): Unit
}

trait SparkAlmanacService extends AlmanacService {

}