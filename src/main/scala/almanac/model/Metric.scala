package almanac.model

import java.text.SimpleDateFormat
import java.util.Date

import almanac.model.Metric._
import almanac.model.TimeSpan._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import GeoHash._

case class Metric(bucket: String, facts: FactMap, span: TimeSpan, timestamp: Long, geohash: String,
                  count: Int, total: Long, max: Long, min: Long) {
  lazy val key = Key(bucket, facts, span, timestamp, geohash)
  lazy val value = Value(count, total, max, min)
  lazy val dateStr = if (span == ALL_TIME) "" else span.dateFormat.format(new Date(timestamp))
  override def toString = f"Metric($bucket,$facts,$span($dateStr),$geohash,$total/$count@[$min,$max])"
}

object Metric {
  type FactMap = Map[String, String]
  case class Key(bucket: String, facts: FactMap, span: TimeSpan, timestamp: Long,
                 geohash: String) {
    def ~ (toSpan: TimeSpan) = Key(bucket, facts, toSpan, toSpan(timestamp), geohash)
    def ~ (toGeoPrecision: Int) = Key(bucket, facts, span, timestamp, geohash ~ toGeoPrecision)
    def - (factKey: String) = Key(bucket, facts - factKey, span, timestamp, geohash)
    def & (groups: Seq[String]) = Key(bucket,
        //work around below as filterKeys returns a MapLike view instead of a serializable map
        Map() ++ facts.filterKeys(groups.contains(_)), span, timestamp, geohash)
  }
  case class Value(count: Int, total: Long, max: Long, min: Long) {
    def + (that: Value) = Value(count + that.count, total + that.total, Math.max(max, that.max), Math.min(min, that.min))
  }

  case class RawBuilder private[model](facts: FactMap, geohash: String = "", optTime: Option[Long]=None) {
    def withFacts(newFacts: (String, String)*) = RawBuilder(facts ++ newFacts, geohash, optTime)
    def withFacts(newFacts: FactMap) = RawBuilder(facts ++ newFacts, geohash, optTime)
    def locate(coordinate: Coordinate) = RawBuilder(facts, coordinate.geohash, optTime)
    def at(timestamp: Long) = RawBuilder(facts, geohash, Some(timestamp))

    def increment(bucket: String) = count(bucket, 1)
    def decrement(bucket: String) = count(bucket, -1)
    def count(bucket: String, amount: Int = 1) = gauge(bucket, amount)
    def gauge(bucket: String, amount: Int) =
      Metric(bucket, facts, RAW, optTime getOrElse System.currentTimeMillis, geohash, 1, amount, amount, amount)
  }

  def apply(key: Key, value: Value): Metric =
    Metric(key.bucket, key.facts, key.span, key.timestamp, key.geohash,
           value.count, value.total, value.max, value.min)

  def metric = RawBuilder(Map())
  def withFacts(facts: (String, String)*) = RawBuilder(Map(facts:_*))
  def withFacts(facts: FactMap) = RawBuilder(facts)
}

sealed abstract class TimeSpan(val strip: Strip, val dateFormatPattern: String = "yyyy") extends Serializable {
  lazy val dateFormat = new SimpleDateFormat(dateFormatPattern)

  private val secondIndex = 2
  lazy val index = values.indexOf(this)

  def apply(timestamp: Long): Long = {
    if (this == ALL_TIME) 0L
    else (secondIndex to this.index foldLeft new DateTime(timestamp, UTC)) {
      (datetime, i) => values(i).strip(datetime)
    }.getMillis
  }
}

object TimeSpan {
  type Strip = (DateTime) => (DateTime)

  case object ALL_TIME  extends TimeSpan(_ withMillis 0)
  case object RAW       extends TimeSpan(x => x, "yyyy/MM/dd HH:mm:ss.S")
  case object SECOND    extends TimeSpan(_ withMillisOfSecond 0, "yyyy/MM/dd HH:mm:ss")
  case object MINUTE    extends TimeSpan(_ withSecondOfMinute 0, "yyyy/MM/dd HH:mm")
  case object HOUR      extends TimeSpan(_ withMinuteOfHour 0, "yyyy/MM/dd HH")
  case object DAY       extends TimeSpan(_ withHourOfDay 0, "yyyy/MM/dd")
  case object MONTH     extends TimeSpan(_ withDayOfMonth 1, "yyyy/MM")
  case object YEAR      extends TimeSpan(_ withMonthOfYear 1)

  lazy val values = Seq(ALL_TIME, RAW, SECOND, MINUTE, HOUR, DAY, MONTH, YEAR)
}