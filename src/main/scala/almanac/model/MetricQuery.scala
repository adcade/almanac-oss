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
  timeFilter: TimeFilter) {

  def unwoundQueries = buckets map (b => MetricsQuery(Set(b), criteria, groupNames, orders, timeFilter))

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append("SELECT ")
    sb.append(buckets)
    sb.append('\n')
    if (criteria != NonCriteria) {
      sb.append("WHERE ")
      sb.append(criteria)
      sb.append('\n')
    }
    if (groupNames.size > 0) {
      sb.append("GROUP BY ")
      sb.append(groupNames)
      sb.append('\n')
    }
    if (orders.size > 0) {
      sb.append("ORDER BY ")
      sb.append(orders)
      sb.append('\n')
    }
    sb.append("TIME ")
    sb.append(timeFilter)
    sb.append('\n')
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
    timeFilter: TimeFilter,
    limit: Int,
    skip: Int) {

    def where(criteria: Criteria) = Builder(buckets, criteria, groupNames, orders, timeFilter, limit, skip)
    def groupBy(facts: String*) = Builder(buckets, criteria, groupNames ++ facts, orders, timeFilter, limit, skip)
    def orderBy(orders: Order*) = Builder(buckets, criteria, groupNames, this.orders ++ orders, timeFilter, limit, skip)
    def orderBy(fact: String, dir: Order.Direction): Builder = orderBy(Order(fact, dir))
    def time(timeFilter: TimeFilter) = Builder(buckets, criteria, groupNames, orders, timeFilter, limit, skip)
    def time(span: TimeSpan, fromTime: Long, toTime: Long): Builder = time(TimeFilter(span, fromTime, toTime))
    def limit(limit: Int): Builder = Builder(buckets, criteria, groupNames, orders, timeFilter, limit, skip)
    def skip(skip: Int) = Builder(buckets, criteria, groupNames, orders, timeFilter, limit, skip)

    lazy val query = MetricsQuery(buckets, criteria, groupNames, orders, timeFilter)
  }

  def select(buckets: String*) = Builder(Set(buckets: _*), NonCriteria, Seq(), Seq(), TimeFilter.ALL_TIME, NO_LIMIT, 0)
}