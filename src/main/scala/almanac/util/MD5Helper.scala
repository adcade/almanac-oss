package almanac.util

import java.security.MessageDigest

object MD5Helper {
  private val md5digest = MessageDigest.getInstance("MD5")

  def md5(source: String): String = md5digest.digest(source.getBytes).map("%02x".format(_)).mkString

  def md5(map: Map[String, String]): String = md5(mapToString(map))

  def mapToString(map: Map[String, String]): String = {
    map.toList.sorted.map {
      case (key, value) => s"$key:$value"
    }.mkString(",")
  }
}
