package almanac.spark

import almanac.AlmanacSettings._
import almanac.model.Metric
import almanac.spark.SparkMetricsAggregator._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

class SparkAlmanacEngine(createRepo: AlmanacMetrcRDDRepositoryFactory,
                         channel: DStreamSource[Metric]) {
  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)

  implicit val sc = SparkContext getOrCreate AlmanacSparkConf
  implicit val repo = createRepo(schedules)

  // FIXME: checkpoint path
  implicit val ssc = StreamingContext getActiveOrCreate { () =>
    new StreamingContext(sc, Milliseconds(SparkStreamingBatchDuration))
  }

  val metricsStream = channel.stream(ssc)
  metricsStream aggregateWithSchedule schedules
  metricsStream window(Seconds(10), Seconds(10)) count() print()

  def start() = {
    ssc.start()
    ssc.awaitTermination()
  }

  def stop() = {
    ssc.stop(true, true)
  }
}
