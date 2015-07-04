package almanac.spark

import almanac.model.GeoFilter._
import almanac.model.Metric.Key
import almanac.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait MetricsAggregator[Source] {
  val source: Source
  def aggregateByTimeSpan(span: TimeSpan) = aggregate(_ ~ span)
  def aggregateByFacts(facts: Seq[String]) = aggregate(_ & facts)
  def aggregateByGeoPrecision(precision: Int) = aggregate(_ ~ precision)
  // def aggregateByBucket(regex: String) = aggregate(_.bucket.matches(regex))

  def aggregate(func: Key => Key): Source
}

trait DStreamSource[M] {
  def stream(ssc: StreamingContext): DStream[M]
}

trait MetricRDDRepository {
  def save(precision: Int, span: TimeSpan, stream: DStream[Metric]): Unit
  def save(precision: Int, span: TimeSpan, stream: RDD[Metric]): Unit

  def saveKeys(stream: DStream[Metric.Key]): Unit
  def saveKeys(stream: RDD[Metric.Key]): Unit

  def read(query: MetricsQuery): RDD[Metric]

  def readFacts(buckets: Set[String], geoFilter: GeoFilter = GlobalFilter, criteria: Criteria): RDD[Map[String, String]]
}
