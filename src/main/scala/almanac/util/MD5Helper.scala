package almanac.util

import java.security.MessageDigest

object MD5Helper {

  def md5(source: String): String = {
    MessageDigest.getInstance("MD5").digest(source.getBytes)
      .map("%02x".format(_))
      .mkString
  }

  def signature(map: Map[String, String]): String = {
    val (keyStr, valueStr) = mapToTuple(map)
    md5(keyStr)+md5(valueStr)
  }

  def mapToTuple(map: Map[String, String]): (String, String) = {
    val (keys, values) = map.toList.sorted.unzip
    (keys.mkString("|"), values.mkString("|"))
  }
}
