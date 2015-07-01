package almanac.util

import java.security.MessageDigest

object MD5Helper {

  def md5(source: String): String = MessageDigest.getInstance("MD5")
      .digest(source.getBytes)
      .map("%02x".format(_))
      .mkString

  def hash(map: Map[String, String]): String = md5(mapToString(map))

  def mapToString(map: Map[String, String]): String =
    map.toList.sorted map { case (key, value) => s"$key:$value" } mkString "|"
}
