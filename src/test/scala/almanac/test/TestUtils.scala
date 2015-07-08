package almanac.test

import java.lang.System._

object TestUtils {

  def time[long](block: => Unit): Long = {
    val t0 = currentTimeMillis
    block
    currentTimeMillis - t0
  }

  def avg(coll: Seq[Long]) = (coll reduce (_+_)) / coll.size
}
