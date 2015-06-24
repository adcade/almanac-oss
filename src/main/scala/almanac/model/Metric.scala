package almanac.model

import java.text.SimpleDateFormat
import java.util.Date
import almanac.model.GeoPrecision._
import almanac.model.Metric._
import almanac.model.TimeSpan._

case class Metric(bucket: String, facts: FactMap, span: TimeSpan, timestamp: Long,
                  precision: GeoPrecision, geolocation: Option[Coordinate],
                  count: Int, total: Long, max: Long, min: Long) {
  lazy val key = Key(bucket, facts, span, timestamp, precision, geolocation)
  lazy val value = Value(count, total, max, min)
  lazy val dateStr = if (span == ALL_TIME) "" else span.dateFormat.format(new Date(timestamp))
  override def toString = f"Metric($bucket,$facts,$span($dateStr),$precision($geolocation),$total/$count@[$min,$max])"
}

object Metric {
  type FactMap = Map[String, String]
  case class Key(bucket: String, facts: FactMap, span: TimeSpan, timestamp: Long,
                 precision: GeoPrecision, geolocation: Option[Coordinate]) {
    def ~ (toSpan: TimeSpan) = Key(bucket, facts, toSpan, toSpan.strip(timestamp), precision, geolocation)
    def ~ (toPrecision: GeoPrecision) = Key(bucket, facts, span, timestamp, toPrecision, toPrecision.strip(geolocation))
    def - (factKey: String) = Key(bucket, facts - factKey, span, timestamp, precision, geolocation)
    def & (groups: Seq[String]) = Key(bucket,
        //work around below as filterKeys returns a MapLike view instead of a serializable map
        Map() ++ facts.filterKeys(groups.contains(_)), span, timestamp, precision, geolocation)
  }
  case class Value(count: Int, total: Long, max: Long, min: Long) {
    def + (that: Value) = Value(count + that.count, total + that.total, Math.max(max, that.max), Math.min(min, that.min))
  }

  case class RawBuilder private[model](facts: FactMap, geolocation: Option[Coordinate]=None) {
    def withFacts(newFacts: (String, String)*) = RawBuilder(facts ++ newFacts)
    def withFacts(newFacts: FactMap) = RawBuilder(facts ++ newFacts)
    def locate(coordinate: Coordinate) = RawBuilder(facts, Some(coordinate))

    def increment(bucket: String) = count(bucket, 1)
    def decrement(bucket: String) = count(bucket, -1)
    def count(bucket: String, amount: Int = 1) = gauge(bucket, amount)
    def gauge(bucket: String, amount: Int) =
      Metric(bucket, facts, RAW, System.currentTimeMillis(), Unrounded, geolocation, 1, amount, amount, amount)
  }

  def apply(key: Key, value: Value): Metric =
    Metric(key.bucket, key.facts, key.span, key.timestamp, key.precision, key.geolocation,
           value.count, value.total, value.max, value.min)

  def metric = RawBuilder(Map())
  def withFacts(facts: (String, String)*) = RawBuilder(Map(facts:_*))
  def withFacts(facts: FactMap) = RawBuilder(facts)
}

case class Coordinate(lat: Double, lng: Double)
case class GeoRect(first: Coordinate, second: Coordinate) {
  lazy val minLat = math.min(first.lat, second.lat)
  lazy val maxLat = math.max(first.lat, second.lat)
  lazy val minLng = math.min(first.lng, second.lng)
  lazy val maxLng = math.max(first.lng, second.lng)
  lazy val topLeft = Coordinate(maxLat, minLng)
  lazy val bottomRight = Coordinate(minLat, maxLng)

  def normalized = GeoRect(topLeft, bottomRight)

  def encompass(coordinate: Coordinate) =
    coordinate.lat >= minLat && coordinate.lat <= maxLat &&
    coordinate.lng >= minLng && coordinate.lng <= maxLat
}

sealed abstract class GeoPrecision(private val digit: Int) extends Serializable {
  def strip(optionalPos: Option[Coordinate]) = optionalPos match {
    case Some(pos) => if (digit == 0) None else if (digit == -1) Some(Coordinate(pos.lat, pos.lng)) else Some(Coordinate(round(pos.lat), round(pos.lng)))
    case None => None
  }

  private def round(value: Double) = math.floor(value * digit) / digit
}

object GeoPrecision {
  case object Unrounded   extends GeoPrecision(-1)
  case object Thousandth extends GeoPrecision(1000)
  case object Hundredth  extends GeoPrecision(100)
  case object Tenth      extends GeoPrecision(10)
  case object Degree     extends GeoPrecision(1)
  case object Wordwide   extends GeoPrecision(0)

  lazy val values = Seq(Unrounded, Thousandth, Hundredth, Tenth, Degree, Wordwide)
  private lazy val lookup = Map(values map {s => (s.digit, s)}: _*)
  def toPrecision(digit: Int) = lookup(digit)
}

sealed abstract class TimeSpan(val millisec: Long, val dateFormatPattern: String = "yyyy")
  extends Ordered[TimeSpan] with Serializable {

  def compare(that: TimeSpan) = (this.millisec - that.millisec) match {
    case x if x > 0 => 1
    case x if x < 0 => -1
    case _ => 0
  }

  lazy val dateFormat = new SimpleDateFormat(dateFormatPattern)
  def strip(timestamp: Long) = if (millisec == 0) 0L else timestamp - timestamp % millisec
}

object TimeSpan {
  case object ALL_TIME  extends TimeSpan(0)
  case object RAW       extends TimeSpan(1, "yyyy/MM/dd HH:mm:ss.S")
  case object SECOND    extends TimeSpan(1000, "yyyy/MM/dd HH:mm:ss")
  case object MINUTE    extends TimeSpan(60000, "yyyy/MM/dd HH:mm")
  case object HOUR      extends TimeSpan(3600000, "yyyy/MM/dd HH")
  case object DAY       extends TimeSpan(86400000, "yyyy/MM/dd")
  case object MONTH     extends TimeSpan(2592000000L, "yyyy/MM")
  case object YEAR      extends TimeSpan(31557600000L)
  case object DECADE    extends TimeSpan(315576000000L)
  case object CENTURY   extends TimeSpan(3155760000000L)
  case object MILLENIUM extends TimeSpan(31557600000000L)
  case object EPOCH     extends TimeSpan(315576000000000L)
  case object UNIX      extends TimeSpan(Long.MaxValue)

  lazy val values = Seq(ALL_TIME, RAW, SECOND, MINUTE, HOUR, DAY, MONTH, YEAR, DECADE, CENTURY, MILLENIUM, EPOCH, UNIX)
  private lazy val lookup = Map(values map {s => (s.millisec, s)}: _*)
  def toTimeSpan(timestamp: Long) = lookup(timestamp)
}