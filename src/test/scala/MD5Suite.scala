import almanac.util.MD5Helper
import almanac.util.MetricsGenerator.generateRaw
import org.scalatest.{FunSuite, Matchers}

class MD5Suite extends FunSuite with Matchers {
  val m = Map("snow" -> "ski", "wave" -> "surf", "mountain" -> "hike")

  test ("md5helper should make map a string") {
    MD5Helper.mapToTuple(m) should be ("mountain|snow|wave","hike|ski|surf")
  }

  test ("md5helper should translate map to md5 string") {
    MD5Helper.signature(m) should be ("6b30c58ee96a0f93e7c887d6b8baa0652a748cd40611b4a3a76521c21563742b")
    MD5Helper.md5("") should be ("d41d8cd98f00b204e9800998ecf8427e")
    MD5Helper.signature(Map.empty[String, String]) should be ("d41d8cd98f00b204e9800998ecf8427ed41d8cd98f00b204e9800998ecf8427e")
  }

  test ("md5 bug hunting") {
    for (_ <- 0 until 10000000) {
      val facts = generateRaw.facts
      print(facts)
      println(MD5Helper.signature(facts))
    }
  }

  test ("device|os md5") {
    println(MD5Helper.md5("device|os"))
  }
}
