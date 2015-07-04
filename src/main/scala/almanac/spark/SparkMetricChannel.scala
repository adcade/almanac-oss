package almanac.spark

import almanac.api.MetricSink
import almanac.model.Metric
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait DStreamSource[M] {
  def stream(ssc: StreamingContext): DStream[M]
}

trait SparkMetricChannel extends MetricSink with DStreamSource[Metric]
