package almanac.spark

import akka.actor._
import almanac.AlmanacSettings._
import almanac.model.Metric
import almanac.persist.CassandraMetricRDDRepository
import almanac.service.MetricsProtocol.{Query, QueryResult, Record}
import almanac.spark.SparkMetricsAggregator._
import almanac.util.RandomMetricsReceiver
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

class SparkAlmanacActor extends Actor with Logging {
  println("start constructor")
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraSeed)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)
    .setAppName("almanac")
    .setMaster(SparkMaster)

  val schedules = AggregationSchedules(GeoSchedules, TimeSchedules)


  val sc = SparkContext getOrCreate conf
  // FIXME: checkpoint path
  val ssc = StreamingContext getActiveOrCreate createNewStreamingContext

  implicit val repo = new CassandraMetricRDDRepository(sc, schedules)

  val receiver = new MetricsReceiver

  //  val metricsStream = ssc receiverStream new MetricsReceiver
  // TODO: dynamic name?
  val metricsStream = ssc receiverStream receiver

  metricsStream aggregateWithSchedule schedules
  metricsStream window(Seconds(10), Seconds(10)) count() print()

  def createNewStreamingContext() = new StreamingContext(sc, Milliseconds(SparkStreamingBatchDuration))
  ssc.start()
  println("finish constructor")

  override def preStart() = {
    println("start to initialize")
    println("finish to initialize")
  }

  def receive: Receive = {
    case Record(metrics) => println("record")
//      receiver receive metrics

    case Query(query) =>
      println("query")
      // TODO: call aggregator do facts group, ordering and limit/paging
      val resultRDD = repo read query
      sender ! QueryResult(resultRDD collect())
  }

  override def postStop() = StreamingContext getActive() foreach {
    _.stop(stopSparkContext = true, stopGracefully = true)
  }
}

class MetricsReceiver extends Receiver[Metric](MEMORY_ONLY) with Logging {
  def receive(metrics: Seq[Metric]) = {
    try {
      logInfo(s"Sending: ${metrics.size} metrics")
      store(metrics.iterator)
      // TODO: restart if stopped ??? what about all the messages received before restart complete?
    } catch {
      case t: Throwable => restart("Error receiving data", t)
    }
  }

  override def onStart(): Unit = {}
  override def onStop(): Unit = {}
}