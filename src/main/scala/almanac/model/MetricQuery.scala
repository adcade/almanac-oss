package almanac.model

import java.util.Date
import almanac.model.GeoFilter.GlobalFilter
import almanac.model.GeoHash._
import almanac.model.TimeFilter.EverFilter
import almanac.model.TimeSpan.EVER

object TimeFilter {
  val EverFilter: TimeFilter = new TimeFilter(EVER, 0, 0)
}

/**
 *
 * @param span
 * @param fromTime
 * @param toTime
 */
case class TimeFilter(span: TimeSpan, fromTime: Long, toTime: Long) {
  override def toString: String =
    if (this == EverFilter) "EVER"
    else "%s\n  FROM %s\n  TO   %s" format (span, new Date(fromTime), new Date(toTime))
}

object GeoFilter {
  val GlobalFilter: GeoFilter = new GeoFilter(GeoRect(LAT_RANGE, LNG_RANGE), GeoHash.GLOBAL)

  def apply(geohash: String, maxPrecision: Int = MAX_PRECISION): GeoFilter = {
    GeoFilter(GeoRect(geohash), maxPrecision)
  }
}

/**
 *
 * @param shape
 * @param maxPrecision
 */
// TODO geofilter for null geohash
case class GeoFilter(shape: GeoShape, maxPrecision: Int) {
  override def toString: String =
    if (this == GlobalFilter) "GLOBAL"
    else "%s max %s" format (shape, maxPrecision)
}

object Order extends Enumeration {
  type Direction = Value
  val ASC, DESC = Value
}

/**
 *
 * @param fact
 * @param dir
 */
case class Order private[model] (fact: String, dir: Order.Direction) {
  override def toString: String = fact + " " + dir
}

/**
 *
 * @param buckets
 * @param criteria
 * @param groupNames
 * @param orders
 * @param geoFilter
 * @param timeFilter
 */
case class MetricsQuery (
  buckets: Set[String],
  criteria: Criteria,
  groupNames: Seq[String],
  orders: Seq[Order],
  geoFilter: GeoFilter,
  timeFilter: TimeFilter) {

  /**
   * unwounded queries with each buckets for caching purpose
   *
   * TODO: make geoFilter also a part of the unwind process
   *
   * @return
   */
  // TODO: need to work on geoFilter
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
    sb append "GEO " append geoFilter append '\n'
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

    /**
     *
     * @param criteria
     * @return
     */
    def where(criteria: Criteria) = Builder(buckets, criteria, groupNames, orders, geoFilter, timeFilter, limit, skip)

    /**
     *
     * @param facts
     * @return
     */
    def groupBy(facts: String*) = Builder(buckets, criteria, groupNames ++ facts, orders, geoFilter, timeFilter, limit, skip)

    /**
     *
     * @param orders
     * @return
     */
    def orderBy(orders: Order*) = Builder(buckets, criteria, groupNames, this.orders ++ orders, geoFilter, timeFilter, limit, skip)

    /**
     *
     * @param fact
     * @param dir
     * @return
     */
    def orderBy(fact: String, dir: Order.Direction): Builder = orderBy(Order(fact, dir))

    /**
     *
     * @param filter
     * @return
     */
    def time(filter: TimeFilter) = Builder(buckets, criteria, groupNames, orders, geoFilter, filter, limit, skip)

    /**
     *
     * @param span
     * @param fromTime
     * @param toTime
     * @return
     */
    def time(span: TimeSpan, fromTime: Long, toTime: Long): Builder = time(TimeFilter(span, fromTime, toTime))

    /**
     *
     * @param filter
     * @return
     */
    def locate(filter: GeoFilter): Builder = Builder(buckets, criteria, groupNames, orders, filter, timeFilter, limit, skip)

    /**
     *
     * @param geohash
     * @return
     */
    def locate(geohash: String): Builder = locate(GeoFilter(geohash))

    /**
     *
     * @param shape
     * @param maxPrecision
     * @return
     */
    def locate(shape: GeoShape, maxPrecision: Int = MAX_PRECISION): Builder = locate(GeoFilter(shape, maxPrecision))

    /**
     *
     * @param limit
     * @return
     */
    def limit(limit: Int): Builder = Builder(buckets, criteria, groupNames, orders, geoFilter, timeFilter, limit, skip)

    /**
     *
     * @param skip
     * @return
     */
    def skip(skip: Int) = Builder(buckets, criteria, groupNames, orders, geoFilter, timeFilter, limit, skip)

    lazy val query = MetricsQuery(buckets, criteria, groupNames, orders, geoFilter, timeFilter)
  }

  /**
   *
   * @param buckets
   * @return
   */
  def select(buckets: String*) = Builder(Set(buckets: _*), NonCriteria, Seq(), Seq(), GlobalFilter, EverFilter, NO_LIMIT, 0)
}