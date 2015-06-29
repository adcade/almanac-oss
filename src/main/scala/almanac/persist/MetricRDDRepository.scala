package almanac.persist

import almanac.model.{Criteria, Metric, TimeSpan}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

trait MetricRDDRepository {
  def save(precision: Int, span: TimeSpan, stream: DStream[Metric]): Unit
  def save(precision: Int, span: TimeSpan, stream: RDD[Metric]): Unit

  def saveFacts(stream: DStream[Metric]): Unit
  def saveFacts(stream: RDD[Metric]): Unit

//  def read(query: MetricsQuery): RDD[Metric]

  def readFacts(criteria: Criteria): RDD[Map[String, String]]
}
