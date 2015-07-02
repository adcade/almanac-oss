package almanac.persist

import almanac.model.GeoFilter._
import almanac.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

trait MetricRDDRepository {
  def save(precision: Int, span: TimeSpan, stream: DStream[Metric]): Unit
  def save(precision: Int, span: TimeSpan, stream: RDD[Metric]): Unit

  def saveKeys(stream: DStream[Metric.Key]): Unit
  def saveKeys(stream: RDD[Metric.Key]): Unit

  def read(query: MetricsQuery): RDD[Metric]

  def readFacts(buckets: Set[String], geoFilter: GeoFilter = WORLDWIDE, criteria: Criteria): RDD[Map[String, String]]
}
