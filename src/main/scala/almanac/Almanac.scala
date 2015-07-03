package almanac

import akka.actor.{ActorSystem, Props}
import almanac.model.GeoRect
import almanac.model.MetricsQuery._
import almanac.model.TimeSpan._
import almanac.service.ActorAlmanacClient
import almanac.spark.SparkAlmanacActor
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global

object Almanac extends App {
  val system = ActorSystem("almanac")
  system.actorOf(Props[SparkAlmanacActor], "almanacActor")
  val almanacActor = system.actorSelection("/user/almanacActor")
  val client = new ActorAlmanacClient(almanacActor)
  val timestamp = new DateTime(2001, 9, 8, 21, 46, 40).getMillis
  client retrieve select("std.impression")
    .locate(GeoRect("dr5"), 7)
    .time(HOUR, HOUR(timestamp), HOUR(timestamp) + 3600000)
    .query foreach println

  //  while (true) {
  //    println("sending")
  //    client record (1 to 10 map (_ => generateRaw): _*)
  //    Thread sleep 1000
  //  }
}
