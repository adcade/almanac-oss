package almanac.util

import almanac.model.Metric._

import scala.util.Random

object MetricsGenerator {
  val devices = Seq("tablet", "pc", "mobile")
  val os = Seq("ios", "windows", "android", "osx", "linux")
  val buckets = Seq("std.imp.richmedia", "std.imp.backup", "std.exit")
  val ran = new Random
  def random(seq: Seq[String]) = if (seq.size > 0) seq(ran.nextInt(seq.size)) else ""

  def generateRaw(times: Int) = 1 to times map {
    _=> withFacts("device"->random(devices), "os"->random(os)) increment random(buckets)
  }
}