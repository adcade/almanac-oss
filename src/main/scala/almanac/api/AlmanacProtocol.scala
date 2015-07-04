package almanac.api

import almanac.model.{Metric, MetricsQuery}

object AlmanacProtocol {
  case class Record(metrics: Seq[Metric])
  case class Query(query: MetricsQuery)

  case class QueryResult(metrics: Seq[Metric])
}
