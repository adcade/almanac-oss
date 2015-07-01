package almanac.test

import almanac.util.MD5Helper._
import org.scalatest.{FunSuite, Matchers}

class MD5Suite extends FunSuite with Matchers {
  val m = Map("snow" -> "ski", "wave" -> "surf", "mountain" -> "hike")

  test ("md5helper should make map a string") {
    mapToString(m) should be ("mountain:hike|snow:ski|wave:surf")
  }

  test ("md5helper should translate map to md5 string") {
    hash(m) should be ("6afe265d8db21603683c6f6aa91f8fd0")
    md5("") should be ("d41d8cd98f00b204e9800998ecf8427e")
    hash(Map.empty[String, String]) should be ("d41d8cd98f00b204e9800998ecf8427e")
  }
}
