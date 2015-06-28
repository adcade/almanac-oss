package almanac

import almanac.model.{Metric, TimeSpan}
import almanac.model.TimeSpan._
import almanac.spark.{MetricStreamHandler, SparkMetricsAggregator}
import almanac.util.MetricsReceiver
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Almanac extends App {
  val conf = new SparkConf().setAppName("almanac")
  val sc = new SparkContext("local[2]", "test", conf)
  val ssc = new StreamingContext(sc, Seconds(1))

//  val timeSchedules = List(SECOND, MINUTE, HOUR, DAY, ALL_TIME)
//  val geoSchedules = List(8, 6, 4, 2, 0)
val timeSchedules = List(HOUR, ALL_TIME)
  val geoSchedules = List(4, 0)
  val metricsStream = ssc receiverStream new MetricsReceiver
  val aggregator = SparkMetricsAggregator(metricsStream, new MetricStreamHandler {
    override def handle(span: TimeSpan, precision: Int, stream: DStream[Metric]) = stream.print()
  }).schedule(geoSchedules, timeSchedules)

  ssc.start()
  ssc.awaitTermination()
}
