package almanac.test

import almanac.util.MD5Helper._
import almanac.util.MetricsGenerator.generateRaw
import org.scalatest.{FunSuite, Matchers}

class MD5Suite extends FunSuite with Matchers {
  val m = Map("snow" -> "ski", "wave" -> "surf", "mountain" -> "hike")

  test ("md5helper should make map a string") {
    mapToTuple(m) should be ("mountain|snow|wave","hike|ski|surf")
  }

  test ("md5helper should translate map to md5 string") {
    signature(m) should be ("6b30c58ee96a0f93e7c887d6b8baa0652a748cd40611b4a3a76521c21563742b")
    md5("") should be ("d41d8cd98f00b204e9800998ecf8427e")
    signature(Map.empty[String, String]) should be ("d41d8cd98f00b204e9800998ecf8427ed41d8cd98f00b204e9800998ecf8427e")
    signature(Map("device"-> "tablet", "os" -> "osx"))
  }

  test ("md5 bug hunting") {
    for (_ <- 0 until 10000000) {
      val facts = generateRaw.facts
      print(facts)
      println(signature(facts))
    }
  }

  test ("device|os md5") {
    println(md5("device|os"))
  }
}
