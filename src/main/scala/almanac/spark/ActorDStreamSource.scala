package almanac.spark

import akka.actor._
import almanac.api.AlmanacProtocol.Record
import almanac.model.Metric
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.ActorHelper

object ActorDStreamSource extends DStreamSource[Metric]{
  val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
  val supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy

  override def stream(ssc: StreamingContext): DStream[Metric] = {
    ssc.actorStream[Metric](Props(classOf[MetricsReceiver], "somePath"), "MetricReceiver", storageLevel, supervisorStrategy)
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
