package almanac.spark

import akka.actor.{Actor, ActorRef}
import almanac.model.Metric._
import almanac.model.TimeFilter._
import almanac.model.{Criteria, MetricsQuery, Metric, TimeSpan}
import almanac.persist.MetricRDDRepository
import almanac.service.AlmanacService
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.ActorHelper

import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag

trait MetricsAggregator[Source] {
  val source: Source
  def aggregateByTimeSpan(span: TimeSpan) = aggregate(_ ~ span)
  def aggregateByFacts(facts: String*) = aggregate(_ & facts)
  def aggregateByGeoPrecision(precision: Int) = aggregate(_ ~ precision)
  // def aggregateByBucket(regex: String) = aggregate(_.bucket.matches(regex))

  def aggregate(func: Key => Key): Source
}

class SparkMetricsAggregator(stream: DStream[Metric], repo: MetricRDDRepository) {
  import SparkMetricsAggregator._

  /**
   * aggregate geo and save result stream
   *
   * @param stream
   * @param precision
   * @param span
   * @return
   */
  private def geoProcess(stream: DStream[Metric], precision: Int, span: TimeSpan) = {
    val resultStream = stream aggregateByGeoPrecision precision
    repo.save(precision, span, resultStream)
    resultStream
  }

  /**
   * aggregate time and handle result stream
   *
   * @param stream
   * @param precision
   * @param span
   * @return
   */
  private def timeProcess(stream: DStream[Metric], precision: Int, span: TimeSpan) = {
    val resultStream = stream aggregateByTimeSpan span
    repo.save(precision, span, resultStream)
    resultStream
  }

  private def keyProcess(stream: DStream[Metric], precision: Int, span: TimeSpan) = repo.saveKeys(stream
    .window(Minutes(1), Minutes(1))
    .aggregateByTimeSpan(ALL_TIME.span)
    .map(_.key))

  /**
   * aggregate the first timeSchedule to the intial stream
   * then aggregate on each level of timeSchedules and geoSchedules like below:
   *
   * Seq(HOUR, DAY, ALL_TIME) Seq(8, 4, WORLDWIDE)
   *
   * in this case HOUR is the intial time span level for aggregation
   *
   * 12, RAW -> initial stream -> 8, HOUR -> DAY -> ALL_TIME
   *                                 |
   *                                 V
   *                              4, HOUR -> DAY -> ALL_TIME
   *                                 |
   *                                 V
   *                      WORLDWIDE, HOUR -> DAY -> ALL_TIME
   *
   * the return value is the last aggregated stream in the above case: WORLDWIDE / ALL_TIME
   * @param schedules time span levels and geo precision levels to be aggregated
   * @return the stream of the last aggregated stream
   */
  def schedule(schedules: AggregationSchedules) = {
    // aggregate first level of time span
    val intialTimeSpan :: otherTimeSchedules = schedules.timeSpans
    val initialStream = stream aggregateByTimeSpan intialTimeSpan
    // aggregate
    (initialStream /: schedules.geoPrecisions) ((tranStream, precision) => {
      // aggregate geo and save result stream
      (geoProcess(tranStream, precision, intialTimeSpan) /: otherTimeSchedules) ( (geoResult, span) => {
        // aggregate fact and handle result stream
        keyProcess(geoResult, precision, span)
        // aggregate time and handle result stream
        timeProcess(geoResult, precision, span)
      })
    })
  }
}

object SparkMetricsAggregator {
  case class AggregationSchedules(geoPrecisions: List[Int], timeSpans: List[TimeSpan])

  implicit class RDDMetricsExtension(val source: RDD[Metric]) extends MetricsAggregator[RDD[Metric]]  {
    override def aggregate(func: Key => Key) =
      source map (m => func(m.key) -> m.value) reduceByKey (_+_) map (t => Metric(t._1, t._2))
  }

  implicit class DStreamMetricsExtension(val source: DStream[Metric]) extends MetricsAggregator[DStream[Metric]] {
    override def aggregate(func: Key => Key) =
      source map (m => func(m.key) -> m.value) reduceByKey (_+_) map (t => Metric(t._1, t._2))
  }
}