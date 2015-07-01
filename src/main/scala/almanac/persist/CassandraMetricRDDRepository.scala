package almanac.persist

import almanac.model.GeoFilter.WORLDWIDE
import almanac.model.TimeFilter.ALL_TIME
import almanac.model._
import almanac.persist.CassandraMetricRDDRepository._
import almanac.spark.SparkMetricsAggregator._
import almanac.util.MD5Helper._
import com.datastax.spark.connector._
import com.datastax.spark.connector.types._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.runtime.universe._

object CassandraMetricRDDRepository {
  object IntToTimeSpanConverter extends TypeConverter[TimeSpan] {
    def targetTypeTag = typeTag[TimeSpan]
    def convertPF = { case code: Int => TimeSpan(code) }
  }

  object TimeSpanToIntConverter extends TypeConverter[Int] {
    def targetTypeTag = typeTag[Int]
    def convertPF = { case span: TimeSpan => span.index }
  }

  private[almanac] val KEYSPACE = "almanac"
  private[almanac] val METRICS_TABLE = "metrics"
  private[almanac] val FACTS_TABLE = "facts"
  private val FACTKEY_OF_EMPTY = "d41d8cd98f00b204e9800998ecf8427e"

  private[almanac] object COLUMN_NAMES {
    val BUCKET    = "bucket"
    val COUNT     = "count"
    val FACT_KEY  = "factkey"
    val FACTS     = "facts"
    val GEOHASH   = "geohash"
    val MAX       = "max"
    val MIN       = "min"
    val SPAN      = "span"
    val TIMESTAMP = "timestamp"
    val TOTAL     = "total"
  }

  val metricToTuple = (m: Metric) => (
    m.bucket,
    m.geohash,
    hash(m.facts),
    m.span.index,
    m.timestamp,
    m.count,
    m.total
    )

  val metricColumns = SomeColumns(
    COLUMN_NAMES.BUCKET,
    COLUMN_NAMES.GEOHASH,
    COLUMN_NAMES.FACT_KEY,
    COLUMN_NAMES.SPAN,
    COLUMN_NAMES.TIMESTAMP,
    COLUMN_NAMES.COUNT,
    COLUMN_NAMES.TOTAL
  )

  case class FactIndex(bucket: String, geohash: String, factkey: String, facts: Map[String, String])
}

class CassandraMetricRDDRepository(sc: SparkContext, schedules: AggregationSchedules) extends Serializable with MetricRDDRepository {
  TypeConverter.registerConverter(IntToTimeSpanConverter)
  TypeConverter.registerConverter(TimeSpanToIntConverter)

  /**
   *
   * @param rdd
   */
  override def saveFacts(rdd: RDD[Metric]) = rdd
      .aggregateByTimeSpan(ALL_TIME.span)
      .distinct()
      .map { mkey => FactIndex(mkey.bucket, mkey.geohash, hash(mkey.facts), mkey.facts) }
      .saveToCassandra(KEYSPACE, FACTS_TABLE)

  /**
   *
   * @param stream
   */
  override def saveFacts(stream: DStream[Metric]) = stream foreachRDD { rdd => saveFacts(rdd) }

  /**
   *
   * @param rdd
   */
  def save(rdd: RDD[Metric]) = rdd map metricToTuple saveToCassandra(KEYSPACE, METRICS_TABLE, metricColumns)

  /**
   *
   * @param precision
   * @param span
   * @param stream
   */
  override def save(precision: Int, span: TimeSpan, stream: RDD[Metric]) = save(stream)

  /**
   *
   * @param precision
   * @param span
   * @param stream
   */
  override def save(precision: Int, span: TimeSpan, stream: DStream[Metric]) = stream foreachRDD { rdd => save(rdd) }

  /**
   *
   * @param buckets
   * @param geoFilter
   * @return
   */
  def readFacts(buckets: Set[String], geoFilter: GeoFilter = WORLDWIDE): RDD[FactIndex] = {
    require(buckets.nonEmpty, "can't pass in empty buckets when reading facts")
    val geoConditions = getGeoConditions(geoFilter)

    buckets.map { bucket => {
      val whereClause = all(
        getBucketConditions(bucket), // bucket = '$bucket'
        geoConditions)               // geohash IN ('$gh1', '$gh2', ...)

      sc.cassandraTable[FactIndex](KEYSPACE, FACTS_TABLE)
        .select(
          COLUMN_NAMES.BUCKET,
          COLUMN_NAMES.GEOHASH,
          COLUMN_NAMES.FACT_KEY,
          COLUMN_NAMES.FACTS)
        .where(whereClause)
    } }.reduceLeft[RDD[FactIndex]](_ union _)
  }

  private def read(whereClause: String): RDD[Metric] = {
    sc.cassandraTable[Metric](KEYSPACE, METRICS_TABLE)
      .select(
        COLUMN_NAMES.BUCKET,
        COLUMN_NAMES.GEOHASH,
        COLUMN_NAMES.SPAN,
        COLUMN_NAMES.TIMESTAMP,
        COLUMN_NAMES.COUNT,
        COLUMN_NAMES.TOTAL)
      .where(whereClause)
  }

  /**
   *
   * @param bucket
   * @param geohash
   * @param span
   * @return
   */
  def read(bucket: String, geohash: String, span: TimeSpan): RDD[Metric] = {
    val whereClause = all(
      getBucketConditions(bucket),
      s"${COLUMN_NAMES.GEOHASH} = '$geohash'",
      s"${COLUMN_NAMES.SPAN} = ${span.index}"
    )

    read(whereClause)
  }

  /**
   *
   * @param query
   * @return
   */
  def read(query: MetricsQuery): RDD[Metric] = {
    readFacts(query.buckets, query.geoFilter)
      .joinWithCassandraTable[CassandraRow](KEYSPACE, METRICS_TABLE)
      .select(
        COLUMN_NAMES.BUCKET,
        COLUMN_NAMES.GEOHASH,
        COLUMN_NAMES.SPAN,
        COLUMN_NAMES.TIMESTAMP,
        COLUMN_NAMES.COUNT,
        COLUMN_NAMES.TOTAL)
      .where(getTimeCondidtions(query.timeFilter))
      .map { case (index, row) =>
        Metric(
          index.bucket,
          index.facts,
          TimeSpan(row.getInt(COLUMN_NAMES.SPAN)),
          row.getLong(COLUMN_NAMES.TIMESTAMP),
          index.geohash,
          row.getInt(COLUMN_NAMES.COUNT),
          row.getLong(COLUMN_NAMES.TOTAL)
        )
      }
  }

  private def all(conditions: String*) = conditions mkString " AND "

  private def getBucketConditions(bucket: String): String = s"${COLUMN_NAMES.BUCKET} = '$bucket'"

  private def getTimeCondidtions(filter: TimeFilter): String = all((
    if (filter == TimeFilter.ALL_TIME)
         Seq(s"${COLUMN_NAMES.TIMESTAMP} = 0" )                  // timestamp = 0

    else Seq(s"${COLUMN_NAMES.TIMESTAMP} >= ${filter.fromTime}", // timestamp >= $fromTime
             s"${COLUMN_NAMES.TIMESTAMP} < ${filter.toTime}")    // timestamp < $toTime

          :+ s"${COLUMN_NAMES.SPAN} = ${filter.span.index}"      // span = $span.index
  ):_*)

  /**
   * if not WORLDWIDE get the geohashes of the geo rect by precision
   * else geohash condition is an empty String
   *
   * @param geoFilter
   * @return
   */
  private def getGeoConditions(geoFilter: GeoFilter) =
    if (geoFilter == WORLDWIDE) s"${COLUMN_NAMES.GEOHASH} = ''" // geohash = ""
    else {
      val precisions = schedules.geoPrecisions filter (_ <= geoFilter.maxPrecision) toSet
      val geohashes = geoFilter.rect.geohashes(precisions)

      // TODO: check if this is empty then throw exception
      if (!geohashes.isEmpty) {
        val geohashList = geohashes.map(gh => s"'$gh'").mkString(", ")
        s"${COLUMN_NAMES.GEOHASH} IN ($geohashList)"            // geohash IN ('$geohashes')
      } else s"${COLUMN_NAMES.GEOHASH} = ''"                    // geohash = ""
    }

  // TODO: look up facts
  override def readFacts(criteria: Criteria): RDD[Map[String, String]] = ???
}
