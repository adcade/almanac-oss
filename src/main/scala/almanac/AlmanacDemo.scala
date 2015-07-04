package almanac

import akka.actor.{ActorSystem, Props}
import almanac.api.ActorAlmanacClient
import almanac.cassandra.CassandraMetricRDDRepositoryFactory
import almanac.kafka.KafkaChannel
import almanac.model.{GeoFilter, GeoRect}
import almanac.model.MetricsQuery._
import almanac.model.TimeSpan._
import almanac.spark.SparkClientActor
import almanac.util.MetricsGenerator.generateRaw
import org.joda.time.DateTime
import scala.concurrent.ExecutionContext.Implicits.global

object AlmanacDemo extends App{
//  val engine = new SparkAlmanacEngine(CassandraMetricRDDRepositoryFactory, RandomDStreamSource)
//  engine.start()

//  val timestamp = new DateTime(2001, 9, 8, 21, 46, 40).getMillis

  val system = ActorSystem("almanac")

  val clientActor = system.actorOf(
    Props(classOf[SparkClientActor], CassandraMetricRDDRepositoryFactory, KafkaChannel), "clientActor"
  )

  val client = new ActorAlmanacClient(system.actorSelection(clientActor.path))

  val timestamp = System.currentTimeMillis()

  for (_ <- 1 to 1000) {
    client record (1 to 10 map {_ =>
      val m = generateRaw
      println(s"$m")
      m
    }: _*)
    Thread sleep 100
  }

  client retrieve select("from.kafka")
    .locate(GeoFilter.GlobalFilter)
    .time(HOUR, HOUR(timestamp), HOUR(timestamp) + 3600000)
    .query foreach println


}
