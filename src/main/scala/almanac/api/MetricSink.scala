package almanac.api

import almanac.model.Metric

trait MetricSink extends AutoCloseable {
  def send(metrics: Seq[Metric])

  override def close(): Unit
}

trait MetricSinkFactory {
  def createSink: MetricSink
}
