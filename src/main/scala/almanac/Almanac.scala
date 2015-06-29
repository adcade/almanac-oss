package almanac

import almanac.model.TimeSpan._
import almanac.persist.RDDCassandraRepository
import almanac.spark.SparkMetricsAggregator
import almanac.spark.SparkMetricsAggregator.AggregationSchedules
import almanac.util.MetricsReceiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Almanac extends App {
  val conf = new SparkConf().setAppName("almanac")
    .set("spark.cassandra.connection.host", "dev")
  val sc = new SparkContext("local[4]", "test", conf)
  val ssc = new StreamingContext(sc, Seconds(1))

//  val timeSchedules = List(SECOND, MINUTE, HOUR, DAY, ALL_TIME)
//  val geoSchedules = List(8, 6, 4, 2, 0)
  val schedules = AggregationSchedules(List(4, 0), List(HOUR, ALL_TIME))
  val metricsStream = ssc receiverStream new MetricsReceiver
//  val aggregator = SparkMetricsAggregator(metricsStream, new MetricStreamHandler {
//    override def handle(span: TimeSpan, precision: Int, stream: DStream[Metric]) = stream.print()
//  }).schedule(geoSchedules, timeSchedules)
  val rddRepo = new RDDCassandraRepository(sc, schedules)
  SparkMetricsAggregator(metricsStream, rddRepo.save).schedule(schedules)

  ssc.start()
  ssc.awaitTermination()
}
