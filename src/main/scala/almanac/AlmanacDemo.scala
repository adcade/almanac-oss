package almanac

import akka.actor.{ActorSystem, Props}
import almanac.api.ActorAlmanacClient
import almanac.cassandra.CassandraMetricRDDRepositoryFactory
import almanac.kafka.{KafkaChannelFactory, KafkaChannel}
import almanac.model.Coordinate
import almanac.model.Criteria.nofact
import almanac.model.GeoFilter.GlobalFilter
import almanac.model.Metric._
import almanac.model.MetricsQuery._
import almanac.model.TimeFilter.EverFilter
import almanac.spark.SparkAlmanacActor
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global

object AlmanacDemo extends App{

  val system = ActorSystem("demo")

  val clientActor = system.actorOf(SparkAlmanacActor.props, "clientActor")

  val client = new ActorAlmanacClient(system.actorSelection(clientActor.path))

//  generateMetrics()

  val startTime = System.currentTimeMillis

  client retrieve select("std.exit")
//    .where(nofact)
    .locate(GlobalFilter)
    .time(EverFilter)
//    .time(MINUTE, HOUR(timestamp), HOUR(timestamp) + 3600000)
    .query foreach { metrics =>
      println(s"Query takes ${System.currentTimeMillis - startTime}")
      metrics foreach println

      system.shutdown()
      system.awaitTermination()
      println("done")
    }

  def generateMetrics() = {

    var count = 0
    for (_ <- 1 to 1000) {
      val t0 = System.currentTimeMillis
      client record (1 to 100 map {_ =>
        val m = metric
          .locate(Coordinate("dr5ru7k3"))
          .at(new DateTime(2015, 7, 4, 11, 20, 30).getMillis)
          .increment("std.exit")
        count += 1
        println(s"$m")
        m
      }: _*)
      //    Thread sleep (t0 + 10 - System.currentTimeMillis)
    }
    println(s"sent: $count")
  }

}
