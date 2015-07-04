package almanac.spark

import almanac.model.GeoFilter._
import almanac.model.Metric.Key
import almanac.model._
import almanac.spark.MetricsAggregator.KeyMapper
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object MetricsAggregator {
  type KeyMapper = Key => Key

  def by(span: TimeSpan): KeyMapper = _ ~ span
  def by(facts: Seq[String]): KeyMapper = _ & facts
  def by(precision: Int): KeyMapper = _ ~ precision
  // def by(regex: String): KeyMapper = _.bucket.matches(regex)

  implicit class JoinedAggregator(keyMapper: KeyMapper) extends Serializable {
    def and(otherKeyMapper: KeyMapper): KeyMapper = key => {
      otherKeyMapper(keyMapper(key))
    }
  }
}

trait MetricsAggregator[Source] {
  val source: Source

  def aggregateMetrics(mapper: KeyMapper): Source
}

trait DStreamSource[M] {
  def stream(ssc: StreamingContext): DStream[M]
}

trait DStreamSourceFactory[M] {
  def createSource: DStreamSource[M]
}

trait MetricRDDRepository extends AutoCloseable{

  def save(precision: Int, span: TimeSpan, stream: DStream[Metric]): Unit
  def save(precision: Int, span: TimeSpan, stream: RDD[Metric]): Unit

  def saveKeys(stream: DStream[Metric.Key]): Unit
  def saveKeys(stream: RDD[Metric.Key]): Unit

  def read(query: MetricsQuery): RDD[Metric]

  def readFacts(buckets: Set[String], geoFilter: GeoFilter = GlobalFilter, criteria: Criteria): RDD[Map[String, String]]
}

trait AlmanacMetrcRDDRepositoryFactory {
  def createRepository(schedules: AggregationSchedules)(implicit sc: SparkContext): MetricRDDRepository
}

/**
 * Time span levels and Geo precision levels to be aggregated
 *
 * please reference to `SparkMetricsAggregator.aggregate` for more detail
 *
 * @param geoPrecisions
 * @param timeSpans
 */
case class AggregationSchedules(geoPrecisions: List[Int], timeSpans: List[TimeSpan])
