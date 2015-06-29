package almanac.util

import almanac.model.Coordinate._
import almanac.model.GeoHash.Bounds
import almanac.model.Metric
import almanac.model.Metric._
import almanac.util.MetricsGenerator._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.streaming.receiver.Receiver

import scala.util.Random

object MetricsGenerator {
  val devices = Seq("tablet", "pc", "mobile")
  val os = Seq("ios", "windows", "android", "osx", "linux")
  val buckets = Seq("std.imp.richmedia", "std.imp.backup", "std.exit")
  val latRange = (40.799, 40.8)
  val lngRange = (73.999, 74.0)
  val ran = new Random
  def random(b: Bounds) = ran.nextDouble * (b._2 - b._1) + b._1
  def random(seq: Seq[String]) = if (seq.size > 0) seq(ran.nextInt(seq.size)) else ""

  def generateRaw = withFacts("device"->random(devices), "os"->random(os))
    .locate(random(latRange) x random(lngRange))
    .increment(random(buckets))

  def generateRawWithFacts = withFacts("device"->random(devices), "os"->random(os))
    .increment(random(buckets))

  def generateRawWithFacts(times: Int) = 1 to times map {
    _=> withFacts("device"->random(devices), "os"->random(os)) increment random(buckets)
  }
  def generateRawWithGeo(times: Int) = 1 to times map {
    _=> metric locate(random(latRange) x random(lngRange)) increment random(buckets)
  }
}

class MetricsReceiver extends Receiver[Metric](MEMORY_ONLY) with Logging {
  private def receive() {
    try {
      logInfo("Start generating random metrics")
      while (!isStopped) {
        val metrics = (0 until 500).map(_=>generateRaw)
        logInfo("sending: " + metrics)
        store(metrics.iterator)
        Thread.sleep(1000)
      }
      logInfo("Stopped generating")
      restart("Trying to restart again")
    } catch {
      case t: Throwable =>
      restart("Error receiving data", t)
    }
  }

  override def onStart() {
    val thread = new Thread("Random Metrics Receiver") {
      override def run = {
        receive()
      }
    }.start()
  }

  override def onStop() {}
}