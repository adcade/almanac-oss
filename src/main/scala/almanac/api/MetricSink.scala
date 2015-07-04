package almanac.api

import almanac.model.Metric

trait MetricSink {
  def send(metrics: Seq[Metric])
}
