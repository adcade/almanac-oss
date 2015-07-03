package almanac.spark

import akka.actor._
import almanac.AlmanacSettings._
import almanac.model.Metric
import almanac.persist.CassandraMetricRDDRepository
import almanac.service.MetricsProtocol.{Query, QueryResult, Record}
import almanac.spark.SparkMetricsAggregator._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.ActorHelper
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

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

  val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2

  val metricsStream = ssc.actorStream[Metric](Props(classOf[MetricsReceiver], self.path), "MetricReceiver", storageLevel, supervisorStrategy)

//  val metricsStream = ssc receiverStream new RandomMetricsReceiver
  // TODO: dynamic name?
//  val metricsStream = ssc receiverStream receiver

  metricsStream aggregateWithSchedule schedules
  metricsStream window(Seconds(10), Seconds(10)) count() print()

  def createNewStreamingContext() = new StreamingContext(sc, Milliseconds(SparkStreamingBatchDuration))
  ssc.start()
  println("finish constructor")

  override def preStart() = {
    println("start to initialize")
    println("finish to initialize")
  }

  val receivers: ListBuffer[ActorRef] = new ListBuffer[ActorRef]()

  def receive: Receive = {
    case Record(metrics) =>
      receivers.foreach(_ ! Record(metrics))

    case Query(query) =>
      println("query")
      // TODO: call aggregator do facts group, ordering and limit/paging
      val resultRDD = repo read query aggregateByFacts query.groupNames
      sender ! QueryResult(resultRDD collect())

    case SubscribeReceiver(receiverActor: ActorRef) =>
      println("received subscribe from %s".format(receiverActor.toString))
      receivers += receiverActor

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      println("received unsubscribe from %s".format(receiverActor.toString))
      receivers.dropWhile(x => x eq receiverActor)
  }

  override def postStop() = StreamingContext getActive() foreach {
    _.stop(stopSparkContext = true, stopGracefully = true)
  }
}

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

class MetricsReceiver(publisherPath: ActorPath) extends Actor with ActorHelper with Logging {
  println(publisherPath)
  println(self.path)
  val publisher = context.actorSelection(publisherPath)

  override def preStart(): Unit = publisher ! SubscribeReceiver(context.self)

  def receive = {
    case Record(metrics) =>
      logInfo(s"Sending: ${metrics.size} metrics")
      store(metrics)
  }

  override def postStop(): Unit = publisher ! UnsubscribeReceiver(context.self)
}