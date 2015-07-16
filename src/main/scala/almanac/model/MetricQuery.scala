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
 * Filter by a time period of certain `TimeSpan` (precision of Time) from `fromTime` to `toTime`
 *
 * @param span the Time Span to be filtered
 * @param fromTime the inclusive start point of the
 * @param toTime the exclusive end point of the time period
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
 * Filter by a geo area (`GeoShape`) with a max precision that is subject to the availability of
 * aggregated geo precisions
 *
 * @param shape the geo area to be filtered
 * @param maxPrecision the max precision to return
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
 * Order by facts only
 * metrics are first ordered by bucket and then time and then geo by default
 * facts ordering is applied after the default ordering
 *
 * @param fact the fact to be ordered
 * @param dir the order direction for the fact, either ASC or DESC
 */
case class Order private[model] (fact: String, dir: Order.Direction) {
  override def toString: String = fact + " " + dir
}

/**
 *
 * @param buckets the buckets to be selected
 * @param criteria the criteria of the facts
 * @param groupNames the fact names to be grouped by
 * @param orders the orders of the facts
 * @param geoFilter the geo filter
 * @param timeFilter the time filter
 */
case class MetricsQuery (
  buckets: Set[String],
  criteria: Criteria,
  groupNames: Seq[String],
  orders: Seq[Order],
  geoFilter: GeoFilter,
  timeFilter: TimeFilter) {

  /**
   * Unwounded queries with each buckets for caching purpose
   *
   * TODO: make geoFilter also a part of the unwind process
   *
   * @return a list of sub-queries that the union of whose results is equivalent to the query itself
   */
  def unwoundQueries = for {
    b <- buckets
  } yield MetricsQuery(Set(b), criteria, groupNames, orders, geoFilter, timeFilter)

  override def toString: String = {
    val sb = new StringBuilder
    sb append "SELECT " append buckets append '\n'
    if (criteria != NonCriteria)
      sb append "WHERE " append criteria append '\n'
    if (groupNames.nonEmpty)
      sb append "GROUP BY " append groupNames append '\n'
    if (orders.nonEmpty)
      sb append "ORDER BY " append orders append '\n'
    sb append "TIME " append timeFilter append '\n'
    sb append "GEO " append geoFilter append '\n'
    sb.toString()
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
     * Filter by facts who meets the criteria
     *
     * @param criteria the criteria of facts
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def where(criteria: Criteria) = Builder(buckets, criteria, groupNames, orders, geoFilter, timeFilter, limit, skip)

    /**
     * Group by facts, reduce metrics where designated facts are the same
     * in other word, any fact that is not included in the parameter list will be eliminated
     *
     * @param facts the facts to be grouped by
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def groupBy(facts: String*) = Builder(buckets, criteria, groupNames ++ facts, orders, geoFilter, timeFilter, limit, skip)

    /**
     * Order by facts only
     * metrics are first ordered by bucket and then time and then geo by default
     * facts ordering is applied after the default ordering
     *
     * @param orders the order object which contains both the fact to be ordered and the direction
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def orderBy(orders: Order*) = Builder(buckets, criteria, groupNames, this.orders ++ orders, geoFilter, timeFilter, limit, skip)

    /**
     * Order by facts only
     * metrics are first ordered by bucket and then time and then geo by default
     * facts ordering is applied after the default ordering
     *
     * @param fact the fact to be ordered
     * @param dir the order direction for the fact, either ASC or DESC
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def orderBy(fact: String, dir: Order.Direction): Builder = orderBy(Order(fact, dir))

    /**
     * Filter the metrics with a `TimeFilter`
     *
     * @param filter the time filter
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def time(filter: TimeFilter) = Builder(buckets, criteria, groupNames, orders, geoFilter, filter, limit, skip)

    /**
     * Filter by a time period of certain `TimeSpan` (precision of Time) from `fromTime` to `toTime`
     *
     * @param span the Time Span to be filtered
     * @param fromTime the inclusive start point of the
     * @param toTime the exclusive end point of the time period
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def time(span: TimeSpan, fromTime: Long, toTime: Long): Builder = time(TimeFilter(span, fromTime, toTime))

    /**
     * Filter the metrics with a `GeoFilter`
     *
     * @param filter the geo filter
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def locate(filter: GeoFilter) = Builder(buckets, criteria, groupNames, orders, filter, timeFilter, limit, skip)

    /**
     * Filter by the geo area represented by the geohash
     *
     * @param geohash the geohash of the geo area to be filtered
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def locate(geohash: String): Builder = locate(GeoFilter(geohash))

    /**
     * Filter by a geo area (`GeoShape`) with a max precision that is subject to the availability of
     * aggregated geo precisions
     *
     * @param shape the geo area to be filtered
     * @param maxPrecision the max precision to return
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def locate(shape: GeoShape, maxPrecision: Int = MAX_PRECISION): Builder = locate(GeoFilter(shape, maxPrecision))

    /**
     * Limit for paginating
     *
     * @param limit maximum metrics returned
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def limit(limit: Int) = Builder(buckets, criteria, groupNames, orders, geoFilter, timeFilter, limit, skip)

    /**
     * Skip for paginating
     *
     * @param skip how many metrics to skip
     * @return a new MetricQuery.Builder that meets the above condition
     */
    def skip(skip: Int): Builder = Builder(buckets, criteria, groupNames, orders, geoFilter, timeFilter, limit, skip)

    lazy val query = MetricsQuery(buckets, criteria, groupNames, orders, geoFilter, timeFilter)
  }

  /**
   * Select all the buckets to be returned
   *
   * @param buckets a list of buckets to be selected
   * @return a new MetricQuery.Builder that meets the above condition
   */
  def select(buckets: String*) = Builder(Set(buckets: _*), NonCriteria, Seq(), Seq(), GlobalFilter, EverFilter, NO_LIMIT, 0)
}