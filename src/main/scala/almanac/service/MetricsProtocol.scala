package almanac.service

import almanac.model.{Metric, MetricsQuery}

object MetricsProtocol {
  case class Record(metrics: Seq[Metric])
  case class Query(query: MetricsQuery)

  case class QueryResult(metrics: Seq[Metric])
}
