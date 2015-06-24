package almanac.model

import java.util.Date

object TimeFilter {
  val ALL_TIME: TimeFilter = new TimeFilter(TimeSpan.ALL_TIME, 0, 0)
}

case class TimeFilter(span: TimeSpan, fromTime: Long, toTime: Long) {
  override def toString: String =
    if (this == TimeFilter.ALL_TIME) "ALL TIME"
    else "%s\n  FROM %s\n  TO   %s" format (span, new Date(fromTime), new Date(toTime))
}

object GeoFilter {
  val WORLDWIDE: GeoFilter = new GeoFilter(GeoPrecision.Wordwide, GeoRect(Coordinate(0, 0), Coordinate(0, 0)))
}

case class GeoFilter(precision: GeoPrecision, rect: GeoRect) {
  override def toString: String =
    if (this == GeoFilter.WORLDWIDE) "WORLDWIDE"
    else "%s\n  rect %s" format (precision, rect)
}

object Order extends Enumeration {
  type Direction = Value
  val ASC, DESC = Value
}

case class Order private[model] (fact: String, dir: Order.Direction) {
  override def toString: String = fact + " " + dir
}

case class MetricsQuery (
  buckets: Set[String],
  criteria: Criteria,
  groupNames: Seq[String],
  orders: Seq[Order],
  geoFilter: GeoFilter,
  timeFilter: TimeFilter) {

  def unwoundQueries = buckets map (b => MetricsQuery(Set(b), criteria, groupNames, orders, geoFilter, timeFilter))

  override def toString: String = {
    val sb = new StringBuilder
    sb append "SELECT " append buckets append '\n'
    if (criteria != NonCriteria)
      sb append "WHERE " append criteria append '\n'
    if (groupNames.size > 0)
      sb append "GROUP BY " append groupNames append '\n'
    if (orders.size > 0)
      sb append "ORDER BY " append orders append '\n'
    sb append "TIME " append timeFilter append '\n'
    sb.toString
  }
}

object MetricsQuery {
  val NO_LIMIT: Int = 0

  case class Builder private[model] (
    buckets: Set[String],
    criteria: Criteria,
    groupNames: Seq[String],
    orders: Seq[Order],
    geoFilter: GeoFilter,
    timeFilter: TimeFilter,
    limit: Int,
    skip: Int) {

    def where(criteria: Criteria) = Builder(buckets, criteria, groupNames, orders, geoFilter, timeFilter, limit, skip)
    def groupBy(facts: String*) = Builder(buckets, criteria, groupNames ++ facts, orders, geoFilter, timeFilter, limit, skip)
    def orderBy(orders: Order*) = Builder(buckets, criteria, groupNames, this.orders ++ orders, geoFilter, timeFilter, limit, skip)
    def orderBy(fact: String, dir: Order.Direction): Builder = orderBy(Order(fact, dir))
    def time(filter: TimeFilter) = Builder(buckets, criteria, groupNames, orders, geoFilter, filter, limit, skip)
    def time(span: TimeSpan, fromTime: Long, toTime: Long): Builder = time(TimeFilter(span, fromTime, toTime))
    def locate(filter: GeoFilter): Builder = Builder(buckets, criteria, groupNames, orders, filter, timeFilter, limit, skip)
    def locate(precision: GeoPrecision, rect: GeoRect): Builder = locate(GeoFilter(precision, rect))
    def limit(limit: Int): Builder = Builder(buckets, criteria, groupNames, orders, geoFilter, timeFilter, limit, skip)
    def skip(skip: Int) = Builder(buckets, criteria, groupNames, orders, geoFilter, timeFilter, limit, skip)

    lazy val query = MetricsQuery(buckets, criteria, groupNames, orders, geoFilter, timeFilter)
  }

  def select(buckets: String*) = Builder(Set(buckets: _*), NonCriteria, Seq(), Seq(), GeoFilter.WORLDWIDE, TimeFilter.ALL_TIME, NO_LIMIT, 0)
}