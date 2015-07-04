package almanac.spark

import almanac.AlmanacSettings._
import almanac.model.Metric
import almanac.spark.SparkMetricsAggregator._
import org.apache.spark.SparkContext
import org.apache.spark.metrics.source
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

class SparkAlmanacEngine(repoFactory: AlmanacMetrcRDDRepositoryFactory,
                         streamFactory: DStreamSourceFactory[Metric]) extends Runnable {
  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)

  // FIXME: checkpointPath
  val ssc = StreamingContext getActiveOrCreate createStreamingContext
  implicit val repo = repoFactory.createRepository(schedules)(ssc.sparkContext)
  streamFactory.createSource.stream(ssc) aggregateWithSchedule schedules stats Seconds(10)

  private def createStreamingContext(): StreamingContext =
    new StreamingContext(AlmanacSparkConf, Milliseconds(SparkStreamingBatchDuration))

  override def run() = {
    ssc.start()
    ssc.awaitTermination()
  }

  def shutdown() = {
    ssc.stop(true, true)
  }
}
