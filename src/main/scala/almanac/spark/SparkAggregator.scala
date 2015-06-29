package almanac.spark

import akka.actor.{Actor, ActorRef}
import almanac.model.Metric._
import almanac.model.{Metric, TimeSpan}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.ActorHelper

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

trait Handler[T, R] {
  def handle(target: T): R
}

class SparkMetricsAggregator(stream: DStream[Metric], streamHandle: (DStream[Metric]) => Unit) {
  import SparkMetricsAggregator._

  def geoProcess(stream: DStream[Metric], precision: Int, span: TimeSpan) = {
    // aggregate geo and handle result stream
    val resultStream = stream aggregateByGeoPrecision precision
    streamHandle(resultStream)
    resultStream
  }

  def timeProcess(stream: DStream[Metric], precision: Int, span: TimeSpan) = {
    // aggregate time and handle result stream
    val resultStream = stream aggregateByTimeSpan span
    streamHandle(resultStream)
    resultStream
  }

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
      (geoProcess(tranStream, precision, intialTimeSpan) /: otherTimeSchedules)(
        timeProcess(_, precision, _)
      )
      tranStream
    })
  }
}

object SparkMetricsAggregator {
  case class AggregationSchedules(geoPrecisions: Seq[Int], timeSpans: Seq[TimeSpan])

  implicit class RDDMetricsExtension(val source: RDD[Metric]) extends MetricsAggregator[RDD[Metric]]  {
    override def aggregate(func: Key => Key) =
      source map (m => func(m.key) -> m.value) reduceByKey (_+_) map (t => Metric(t._1, t._2))
  }

  implicit class DStreamMetricsExtension(val source: DStream[Metric]) extends MetricsAggregator[DStream[Metric]] {
    override def aggregate(func: Key => Key) =
      source map (m => func(m.key) -> m.value) reduceByKey (_+_) map (t => Metric(t._1, t._2))
  }

  def apply(stream: DStream[Metric], streamHandle: (DStream[Metric]) => Unit) =
    new SparkMetricsAggregator(stream, streamHandle)

}

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

import almanac.service.MetricsProtocol._

class MetricsActorReceiver[T: ClassTag] (urlOfPublisher: String)
  extends Actor with ActorHelper {
  lazy private val publisher = context.actorSelection(urlOfPublisher)

  override def preStart(): Unit = publisher ! SubscribeReceiver(context.self)

  def receive = {
    case Record(metrics) => store(metrics)
  }

  override def postStop(): Unit = publisher ! UnsubscribeReceiver(context.self)
}