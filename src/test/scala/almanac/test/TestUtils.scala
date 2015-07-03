package almanac.test

import java.lang.System._

import scalaz.syntax.std.all

object TestUtils {

  def time[long](block: => Unit): Long = {
    val t0 = currentTimeMillis
    block
    currentTimeMillis - t0
  }

  def avg(coll: Seq[Long]) = (coll reduce (_+_)) / coll.size
}
