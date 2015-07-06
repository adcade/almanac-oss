package almanac.model

import java.text.SimpleDateFormat
import java.util.Date

import almanac.model.GeoHash._
import almanac.model.Metric._
import almanac.model.TimeSpan._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC

case class Metric(bucket: String, facts: Map[String, String], span: TimeSpan, timestamp: Long, geohash: String,
                  count: Int, total: Long) {
  /**
   * The key part of the metrics
   */
  lazy val key = Key(bucket, facts, span, timestamp, geohash)

  /**
   * The value part of the metrics
   */
  lazy val value = Value(count, total)

  /**
   * the the datetime of this metrics formated by the span
   * @return the datetime string
   */
  def dateStr = if (span == EVER) "" else span.dateFormat.format(new Date(timestamp))

  override def toString = f"Metric($bucket,$facts,$span($dateStr),$geohash,$total/$count)"
}

object Metric {
  type FactMap = Map[String, String]
  case class Key(bucket: String, facts: FactMap, span: TimeSpan, timestamp: Long,
                 geohash: String) {
    /**
     *
     * @param toSpan
     * @return
     */
    def ~ (toSpan: TimeSpan) = Key(bucket, facts, toSpan, toSpan(timestamp), geohash)

    /**
     *
     * @param toGeoPrecision
     * @return
     */
    def ~ (toGeoPrecision: Int) = Key(bucket, facts, span, timestamp,
      geohash ~ toGeoPrecision
    )

    /**
     *
     * @param factKey
     * @return
     */
    def - (factKey: String) = Key(bucket, facts - factKey, span, timestamp, geohash)

    /**
     *
     * @param groups
     * @return
     */
    def & (groups: Seq[String]) = Key(bucket,
        //work around below as filterKeys returns a MapLike view instead of a serializable map
        Map() ++ facts.filterKeys(groups.contains(_)), span, timestamp, geohash)
  }
  case class Value(count: Int, total: Long) {
    /**
     *
     * @param that
     * @return
     */
    def + (that: Value) = Value(count + that.count, total + that.total)
  }

  // FIXME: the null of geohash is driving me crazy! but Kryo in spark is having problem serializing Option[String]
  private[model] case class RawBuilder private[model](facts: FactMap, geohash: String = nullGeohash(12), optTime: Option[Long]=None) {
    /**
     *
     * @param newFacts
     * @return
     */
    def withFacts(newFacts: (String, String)*) = RawBuilder(facts ++ newFacts, geohash, optTime)

    /**
     *
     * @param newFacts
     * @return
     */
    def withFacts(newFacts: FactMap) = RawBuilder(facts ++ newFacts, geohash, optTime)

    /**
     *
     * @param coordinate
     * @return
     */
    def locate(coordinate: Coordinate) = RawBuilder(facts, coordinate.geohash, optTime)

    /**
     *
     * @param timestamp
     * @return
     */
    def at(timestamp: Long) = RawBuilder(facts, geohash, Some(timestamp))

    /**
     *
     * @param bucket
     * @return
     */
    def increment(bucket: String, amount: Int = 1) = gauge(bucket, 1)

    /**
     *
     * @param bucket
     * @return
     */
    def decrement(bucket: String, amount: Int = -1) = gauge(bucket, -1)

    /**
     *
     * @param bucket
     * @param amount
     * @return
     */
    def gauge(bucket: String, amount: Int) =
      Metric(bucket, facts, RAW, optTime getOrElse System.currentTimeMillis, geohash, 1, amount)
  }

  /**
   *
   * @param key
   * @param value
   * @return
   */
  def apply(key: Key, value: Value): Metric =
    Metric(key.bucket, key.facts, key.span, key.timestamp, key.geohash,
           value.count, value.total)

  /**
   *
   * @return
   */
  def metric = RawBuilder(Map())

  /**
   *
   * @return
   */
  def locate(coordinate: Coordinate) = RawBuilder(Map(), coordinate.geohash)

  /**
   *
   * @param facts
   * @return
   */
  def withFacts(facts: (String, String)*) = RawBuilder(Map(facts:_*))

  /**
   *
   * @param facts
   * @return
   */
  def withFacts(facts: FactMap) = RawBuilder(facts)
}

sealed abstract class TimeSpan(val strip: Strip, val dateFormatPattern: String = "yyyy")
  extends Ordered[TimeSpan] with Serializable {

  lazy val dateFormat = new SimpleDateFormat(dateFormatPattern)

  lazy val index = values.indexOf(this)
  override def compare(that: TimeSpan): Int = index - that.index

  /**
   * to strip the extra bits from a DateTime to make a new DateTime that is of the beginning of the TimeSpan
   *
   * note for Day and above, it's always in UTC time
   *
   * @param timestamp
   * @return
   */
  def apply(timestamp: Long): Long = {
    if (this == EVER) 0L
    else (SECOND.index to this.index foldLeft new DateTime(timestamp, UTC)) {
      (datetime, i) => values(i).strip(datetime)
    }.getMillis
  }
}

/**
 * Enumeration all the possible aggregation
 */
object TimeSpan {

  /**
   * to Strip the extra bits from a DateTime to make a new DateTime that is of the beginning of the TimeSpan
   */
  private type Strip = (DateTime) => (DateTime)

  case object RAW       extends TimeSpan(x => x, "yyyy/MM/dd HH:mm:ss.S")
  case object SECOND    extends TimeSpan(_ withMillisOfSecond 0, "yyyy/MM/dd HH:mm:ss")
  case object MINUTE    extends TimeSpan(_ withSecondOfMinute 0, "yyyy/MM/dd HH:mm")
  case object HOUR      extends TimeSpan(_ withMinuteOfHour 0, "yyyy/MM/dd HH")
  case object DAY       extends TimeSpan(_ withHourOfDay 0, "yyyy/MM/dd")
  case object MONTH     extends TimeSpan(_ withDayOfMonth 1, "yyyy/MM")
  case object YEAR      extends TimeSpan(_ withMonthOfYear 1)
  case object EVER      extends TimeSpan(_ withMillis 0)

  private lazy val values: Seq[TimeSpan with Product] = Seq(RAW, SECOND, MINUTE, HOUR, DAY, MONTH, YEAR, EVER)

  /**
   * for convert from index to TimeSpan, just do `TimeSpan(index)`
   *
   * @param index index of the TimeSpan for serialization purpose
   * @return
   */
  def apply(index: Int) = values(index)

  private lazy val lookup: Map[String, TimeSpan] = (values map (s => s.toString -> s)).toMap

  /**
   * for convert from name String to TimeSpan, just do `TimeSpan(name)`
   *
   * @param name
   * @return
   */
  def apply(name: String) = lookup(name)
}