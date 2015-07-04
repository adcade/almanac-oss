package almanac.cassandra

import almanac.AlmanacSettings
import almanac.model.GeoFilter.GlobalFilter
import almanac.model.TimeFilter.EverFilter
import almanac.model._
import almanac.spark.{MetricRDDRepository, AggregationSchedules, AlmanacMetrcRDDRepositoryFactory}
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

  private[almanac] val KEYSPACE = AlmanacSettings.CassandraKeyspace
  private[almanac] val METRICS_TABLE = AlmanacSettings.CassandraMetricsTable
  private[almanac] val FACTS_TABLE = AlmanacSettings.CassandraFactsTable

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

  case class KeyIndex(bucket: String, geohash: String, factkey: String, facts: Map[String, String])
}

object CassandraMetricRDDRepositoryFactory extends AlmanacMetrcRDDRepositoryFactory {
  override def apply(schedules: AggregationSchedules)(implicit sc: SparkContext): MetricRDDRepository = {
    new CassandraMetricRDDRepository(sc, schedules)
  }
}

class CassandraMetricRDDRepository(sc: SparkContext, schedules: AggregationSchedules) extends Serializable with MetricRDDRepository {
  import CassandraMetricRDDRepository._
  TypeConverter.registerConverter(IntToTimeSpanConverter)
  TypeConverter.registerConverter(TimeSpanToIntConverter)

  /**
   *
   * @param rdd
   */
  override def saveKeys(rdd: RDD[Metric.Key]) = rdd
      .map { k => KeyIndex(k.bucket, k.geohash, hash(k.facts), k.facts) }
      .saveToCassandra(KEYSPACE, FACTS_TABLE)

  /**
   *
   * @param stream
   */
  override def saveKeys(stream: DStream[Metric.Key]) = stream foreachRDD (saveKeys(_))

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

  private[almanac] def readKeys(buckets: Set[String], geoFilter: GeoFilter, criteria: Criteria): RDD[KeyIndex] = {
    require(buckets.nonEmpty, "can't pass in empty buckets when reading facts")
    val geohashes = getGeoHashes(geoFilter)

    buckets.map(readKeys(_, geohashes, criteria)).reduceLeft[RDD[KeyIndex]] (_ union _)
  }

  private[almanac] def readKeys(bucket: String, geohashes: Set[String], criteria: Criteria): RDD[KeyIndex] =
    // TODO: read facts by Criteria
    sc.cassandraTable[KeyIndex](KEYSPACE, FACTS_TABLE)
      .select(
        COLUMN_NAMES.BUCKET,
        COLUMN_NAMES.GEOHASH,
        COLUMN_NAMES.FACT_KEY,
        COLUMN_NAMES.FACTS)
      .where(all(
        getBucketConditions(bucket), // bucket = '$bucket'
        getGeoConditions(geohashes))) // geohash IN ('$gh1', '$gh2', ...))

  /**
   *
   * @param buckets
   * @param geoFilter
   * @return
   */
  def readFacts(buckets: Set[String], geoFilter: GeoFilter = GlobalFilter,
                criteria: Criteria = NonCriteria): RDD[Map[String, String]] =
    readKeys(buckets, geoFilter, criteria) map (_.facts)

  private def read(whereClause: String): RDD[Metric] = sc.cassandraTable[Metric](KEYSPACE, METRICS_TABLE).select(
    COLUMN_NAMES.BUCKET,
    COLUMN_NAMES.GEOHASH,
    COLUMN_NAMES.SPAN,
    COLUMN_NAMES.TIMESTAMP,
    COLUMN_NAMES.COUNT,
    COLUMN_NAMES.TOTAL
  ).where(whereClause)

  /**
   *
   * @param bucket
   * @param geohash
   * @param timeFilter
   * @return
   */
  def read(bucket: String, geohash: String, timeFilter: TimeFilter): RDD[Metric] = this read all(
    getBucketConditions(bucket),
    getGeoConditions(geohash),
    getTimeCondidtions(timeFilter)
  )

  private def noFactsJoinRead(query: MetricsQuery): RDD[Metric] = {
    val partitionKeys = for {
      bucket <- query.buckets
      geohash <- getGeoHashes(query.geoFilter)
    } yield KeyIndex(bucket, geohash, FACTKEY_OF_EMPTY, Map())

    val keyIndice = sc.parallelize(partitionKeys toSeq).repartitionByCassandraReplica(KEYSPACE, METRICS_TABLE)

    joinRead(keyIndice, query.timeFilter)
  }

  private def withFactsJoinRead(query: MetricsQuery): RDD[Metric] = {
    val keyIndice = readKeys(query.buckets, query.geoFilter, query.criteria)

    joinRead(keyIndice, query.timeFilter)
  }

  private def joinRead(rdd: RDD[KeyIndex], timeFilter: TimeFilter) = rdd
    .joinWithCassandraTable[CassandraRow] (KEYSPACE, METRICS_TABLE)
    .select(
      COLUMN_NAMES.BUCKET,
      COLUMN_NAMES.GEOHASH,
      COLUMN_NAMES.SPAN,
      COLUMN_NAMES.TIMESTAMP,
      COLUMN_NAMES.COUNT,
      COLUMN_NAMES.TOTAL)
    .where(getTimeCondidtions(timeFilter))
    .map { case (index, row) => Metric(
      index.bucket,
      index.facts,
      TimeSpan(row.getInt(COLUMN_NAMES.SPAN)),
      row.getLong(COLUMN_NAMES.TIMESTAMP),
      index.geohash,
      row.getInt(COLUMN_NAMES.COUNT),
      row.getLong(COLUMN_NAMES.TOTAL))
    }

  /**
   *
   * @param query
   * @return
   */
  def read(query: MetricsQuery): RDD[Metric] = query.criteria match {
    case NonFactCriteria => noFactsJoinRead(query)
    case _ => withFactsJoinRead(query)
  }

  private def all(conditions: String*) = conditions mkString " AND "

  private def getBucketConditions(bucket: String): String = s"${COLUMN_NAMES.BUCKET} = '$bucket'"

  private def getTimeCondidtions(filter: TimeFilter): String = all((
    if (filter == EverFilter)
         Seq(s"${COLUMN_NAMES.TIMESTAMP} = 0",                   // timestamp = 0
             s"${COLUMN_NAMES.SPAN} = ${filter.span.index}")     // span = $span.index

    else Seq(s"${COLUMN_NAMES.TIMESTAMP} >= ${filter.fromTime}", // timestamp >= $fromTime
             s"${COLUMN_NAMES.TIMESTAMP} < ${filter.toTime}",    // timestamp < $toTime
             s"${COLUMN_NAMES.SPAN} = ${filter.span.index}")     // span = $span.index
  ):_*)

  /**
   *
   * @param geohash
   * @return
   */
  private def getGeoConditions(geohash: String) = s"${COLUMN_NAMES.GEOHASH} = '$geohash'"

  /**
   * if not GLOBAL get the geohashes of the geo rect by precision
   * else geohash condition is an empty String
   *
   * @param geoFilter
   * @return
   */
  private def getGeoConditions(geoFilter: GeoFilter): String =
    if (geoFilter == GlobalFilter) getGeoConditions("") // geohash = ""
    else {
      val geohashes = getGeoHashes(geoFilter)
      getGeoConditions(geohashes)
    }

  private def getGeoConditions(geohashes: Set[String]): String =
    // TODO: check if this is empty then throw exception not worldwide
    if (geohashes.nonEmpty) {
      val geohashList = geohashes.map(gh => s"'$gh'").mkString(", ")
      s"${COLUMN_NAMES.GEOHASH} IN ($geohashList)" // geohash IN ('$geohashes')
    } else getGeoConditions("")                    // geohash = ""

  private def getGeoHashes(geoFilter: GeoFilter): Set[String] = {
    val precisions = schedules.geoPrecisions.filter(_ <= geoFilter.maxPrecision).toSet
    geoFilter.rect.geohashes(precisions)
  }
}
