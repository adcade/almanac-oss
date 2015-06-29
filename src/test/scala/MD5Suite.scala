import almanac.util.MD5Helper
import org.scalatest.{Matchers, FunSuite}

class MD5Suite extends FunSuite with Matchers {
  val m = Map("snow" -> "ski", "wave" -> "surf", "mountain" -> "hike")

  test ("md5helper should make map a string") {
    MD5Helper.mapToString(m) should be ("mountain:hike,snow:ski,wave:surf")
  }

  test ("md5helper should translate map to md5 string") {
    MD5Helper.md5(m) should be ("0a6091efbaa76cceaf0bd6929aa7f5c1")
    MD5Helper.md5("") should be ("d41d8cd98f00b204e9800998ecf8427e")
    MD5Helper.md5(Map.empty[String, String]) should be ("d41d8cd98f00b204e9800998ecf8427e")
  }
}
