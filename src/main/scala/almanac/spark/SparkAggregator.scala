package almanac.spark

import almanac.model.Metric.Key
import almanac.model.TimeSpan.EVER
import almanac.model._
import almanac.spark.MetricsAggregator._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

object SparkMetricsAggregator {

  implicit class RDDMetricsExtension(val source: RDD[Metric]) extends MetricsAggregator[RDD[Metric]]  {
    override def aggregateMetrics(func: KeyMapper) =
      source map (m => func(m.key) -> m.value) reduceByKey (_+_) map (t => Metric(t._1, t._2))
  }

  implicit class DStreamMetricsExtension(val source: DStream[Metric]) extends MetricsAggregator[DStream[Metric]] {
    override def aggregateMetrics(func: KeyMapper): DStream[Metric] = {
      source map (m => func(m.key) -> m.value) reduceByKey (_ + _) map (t => Metric(t._1, t._2))
    }

    def stats(interval: Duration) = {
      source
        .map(m => m.bucket -> m.count)
        .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(10), Seconds(10))
        .print()
      source
    }

    /**
     * aggregate geo-precision/time-span and save result stream
     *
     * @param precision
     * @param span
     * @return
     */
    def saveMetrics(precision: Int, span: TimeSpan)(implicit repo: MetricRDDRepository): DStream[Metric] = {
      println(s"save aggregated metrics of precision: $precision, span: $span")
      repo.save(precision, span, source)
      source
    }

    def saveKeys(precision: Int, span: TimeSpan)(implicit repo: MetricRDDRepository): DStream[Key] = {
      // TODO: configuration of window span
      val keyStream = source window(Minutes(1), Minutes(1)) aggregateMetrics by(EVER) map (_.key)
      println(s"save keys of precision: $precision")
      repo.saveKeys(keyStream)
      keyStream
    }

    /**
     * aggregate the first timeSchedule to the intial stream
     * then aggregate on each level of timeSchedules and geoSchedules like below:
     *
     * Seq(HOUR, DAY, EVER) Seq(8, 4, GLOBAL)
     *
     * (12, RAW) -> (12, HOUR) -> (8, HOUR) -> 8, DAY) -> 8, EVER) -> 8, EVER) -> 8, EVER)
     *                               |
     *                               v
     *                            (4, HOUR) -> 4, DAY) -> 4, EVER) -> 4, EVER) -> 4, EVER)
     *                               |
     *                               v
     *                   GLOBAL / (0, HOUR) -> 0, DAY) -> 0, EVER) -> 0, EVER) -> 0, EVER)
     *
     * the return value is the last aggregated stream in the above case: GLOBAL / EVER
     *
     * @param repo the stream to be aggregated
     * @param schedules
     * @return the stream of the last aggregated stream
     */
    def aggregateWithSchedule(schedules: AggregationSchedules = defaultSchedules)(implicit repo: MetricRDDRepository) = {
      val spans = schedules.timeSpans.sorted
      val precisions = schedules.geoPrecisions.sorted.reverse

      val firstStream =
        source aggregateMetrics by(spans.head)

      (firstStream /: precisions) { (previousGeoStream, precision) =>
        val nextStream = previousGeoStream aggregateMetrics by(precision) saveMetrics(precision, spans.head)
        nextStream saveKeys (precision, spans.last)

        (nextStream /: spans.tail) { (previousSpanStream, span) =>
          previousSpanStream aggregateMetrics by(span) saveMetrics(precision, span)
        }
        nextStream
      }
    }
  }

  val defaultSchedules = AggregationSchedules(List(GeoHash.GLOBAL), List(TimeSpan.EVER))

}