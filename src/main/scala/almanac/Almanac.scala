package almanac

import akka.actor.{ActorSystem, Props}
import almanac.service.ActorAlmanacClient
import almanac.spark.SparkAlmanacActor
import almanac.util.MetricsGenerator._

object Almanac extends App {
  val system = ActorSystem("almanac")
  system.actorOf(Props[SparkAlmanacActor], "almanacActor")
  val almanacActor = system.actorSelection("/user/almanacActor")
  val client = new ActorAlmanacClient(almanacActor)

  client.record(generateRaw)
}
