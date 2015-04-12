package almanac.model

case class Metric private (
  bucket: String,
  facts: Map[String, String],
  timestamp: Long,
  span: TimeSpan,
  count: Int,
  total: Long,
  max: Long,
  min: Long,
  stdev: Double
){
  override def clone = Metric(bucket, facts, timestamp, span, count, total, max, min, stdev)
}

object Metric {
  case class Builder private[model] (
    bucket: String,
    facts: Map[String, String],
    timestamp: Long,
    span: TimeSpan,
    count: Int,
    total: Long,
    max: Long,
    min: Long,
    stdev: Double) {

    def fact(key: String, value: String): Builder = fact(Map(key -> value))
    def fact(newFacts: (String, String)*): Builder = fact(Map(newFacts:_*))
    def fact(newFacts: Map[String, String]) = Builder(bucket, facts ++ newFacts, timestamp, span, count, total, max, min, stdev)
    def timestamp(v: Long): Builder = Builder(bucket, facts, v, span, count, total, max, min, stdev)
    def span(v: TimeSpan)      = Builder(bucket, facts, timestamp, v, count, total, max, min, stdev)
    def count(count: Int): Builder          = Builder(bucket, facts, timestamp, span, count, total, max, min, stdev)
    def total(total: Long): Builder         = Builder(bucket, facts, timestamp, span, count, total, max, min, stdev)
    def max(max: Long): Builder             = Builder(bucket, facts, timestamp, span, count, total, max, min, stdev)
    def min(min: Long): Builder             = Builder(bucket, facts, timestamp, span, count, total, max, min, stdev)
    def stdev(stdev: Double): Builder       = Builder(bucket, facts, timestamp, span, count, total, max, min, stdev)

    def metric: Metric =
      if (count == 1) Metric(bucket, facts, timestamp, span, count, total, total, total, stdev)
      else            Metric(bucket, facts, timestamp, span, count, total, max,   min,   stdev)
  }

  def bucket(bucket: String) = Builder(bucket, Map(), System.currentTimeMillis(), TimeSpan.RAW, 1, 0, 0, 0, .0)
}

sealed abstract class TimeSpan (val millisec: Long) extends Ordered[TimeSpan] {
  def compare(that: TimeSpan) = (this.millisec - that.millisec) match {
    case x if x > 0 => 1
    case x if x < 0 => -1
    case _ => 0
  }

  def strip(timestamp: Long) = if (millisec == 0) 0L else timestamp - timestamp % millisec
}

object TimeSpan {
  case object ALL_TIME  extends TimeSpan(0)
  case object RAW       extends TimeSpan(1)
  case object SECOND    extends TimeSpan(1000)
  case object MINUTE    extends TimeSpan(60000)
  case object HOUR      extends TimeSpan(3600000)
  case object DAY       extends TimeSpan(86400000)
  case object MONTH     extends TimeSpan(2592000000L)
  case object YEAR      extends TimeSpan(31557600000L)
  case object DECADE    extends TimeSpan(315576000000L)
  case object CENTURY   extends TimeSpan(3155760000000L)
  case object MILLENIUM extends TimeSpan(31557600000000L)
  case object EPOCH     extends TimeSpan(315576000000000L)
  case object UNIX      extends TimeSpan(Long.MaxValue)

  lazy val values = Seq(ALL_TIME, RAW, SECOND, MINUTE, HOUR, DAY, MONTH, YEAR, DECADE, CENTURY, MILLENIUM, EPOCH, UNIX)
  private lazy val lookup = Map( values map { s => (s.millisec, s)}: _*)
  def toTimeSpan(timestamp: Long) = lookup(timestamp)
}
