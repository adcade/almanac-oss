package almanac.persist

import java.util.UUID

import almanac.model.GeoFilter.WORLDWIDE
import almanac.model.TimeFilter.ALL_TIME
import almanac.model._
import almanac.persist.RDDCassandraRepository._
import almanac.spark.SparkMetricsAggregator.AggregationSchedules
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.types._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.reflect.runtime.universe._

object RDDCassandraRepository {
  object IntToTimeSpanConverter extends TypeConverter[TimeSpan] {
    def targetTypeTag = typeTag[TimeSpan]
    def convertPF = { case code: Int => TimeSpan.values(code) }
  }

  object TimeSpanToIntConverter extends TypeConverter[Int] {
    def targetTypeTag = typeTag[Int]
    def convertPF = { case span: TimeSpan => span.index }
  }

  private val KEYSPACE = "almanac"
  private val TABLE_NAME = "metrics"

  private object COLUMN_NAMES {
    val BUCKET = "bucket"
    val COUNT = "count"
    val FACTKEY = "factkey"
    val FACTS = "facts"
    val GEOHASH = "geohash"
    val MAX = "max"
    val MIN = "min"
    val SPAN = "span"
    val TIMESTAMP = "timestamp"
    val TOTAL = "total"
  }
}

class RDDCassandraRepository(sc: SparkContext, schedules: AggregationSchedules) extends Serializable {
  TypeConverter.registerConverter(IntToTimeSpanConverter)
  TypeConverter.registerConverter(TimeSpanToIntConverter)

  def metricToTuple(m: Metric) = (
    m.bucket,
    UUID.randomUUID().toString,
    m.facts,
    m.span.index,
    m.timestamp,
    m.geohash,
    m.count,
    m.total,
    m.max,
    m.min)

  val someColumns = SomeColumns(
    COLUMN_NAMES.BUCKET,
    COLUMN_NAMES.FACTKEY,
    COLUMN_NAMES.FACTS,
    COLUMN_NAMES.SPAN,
    COLUMN_NAMES.TIMESTAMP,
    COLUMN_NAMES.GEOHASH,
    COLUMN_NAMES.COUNT,
    COLUMN_NAMES.TOTAL,
    COLUMN_NAMES.MAX,
    COLUMN_NAMES.MIN
  )

  def save(rdd: RDD[Metric]) = rdd.map{ m=>(
      m.bucket,
      "",
      m.facts,
      m.span.index,
      m.timestamp,
      m.geohash,
      m.count,
      m.total,
      m.max,
      m.min)
  }.saveToCassandra(KEYSPACE, TABLE_NAME, someColumns)

  def save(stream: DStream[Metric]) = stream.map { m=>(
      m.bucket,
      "",
      m.facts,
      m.span.index,
      m.timestamp,
      m.geohash,
      m.count,
      m.total,
      m.max,
      m.min)
  }.saveToCassandra(KEYSPACE, TABLE_NAME, someColumns)

  private def read(whereClause: String): RDD[Metric] = {
    sc.cassandraTable[Metric](KEYSPACE, TABLE_NAME)
      .select(
        COLUMN_NAMES.BUCKET,
        COLUMN_NAMES.FACTS,
        COLUMN_NAMES.GEOHASH,
        COLUMN_NAMES.SPAN,
        COLUMN_NAMES.TIMESTAMP,
        COLUMN_NAMES.COUNT,
        COLUMN_NAMES.TOTAL,
        COLUMN_NAMES.MAX,
        COLUMN_NAMES.MIN)
      .where(whereClause)
  }

  def read(bucket: String, geoHash: String, span: TimeSpan): RDD[Metric] = {
    val whereClause = List(
      s"${COLUMN_NAMES.BUCKET} = '$bucket'",
      s"${COLUMN_NAMES.GEOHASH} = '$geoHash'",
      s"${COLUMN_NAMES.SPAN} = ${span.index}"
    ).mkString(" and ")

    read(whereClause)
  }

  def read(query: MetricsQuery): RDD[Metric] = {
    query.buckets.map { bucket =>
      val whereClause = toWhereClause(bucket, query.timeFilter, query.geoFilter)

      println(whereClause)

      read(whereClause)
    } reduceLeft (_ union _)
  }

  private def toWhereClause(bucket: String, timeFilter: TimeFilter, geoFilter: GeoFilter) = {
    val conditions = mutable.Buffer(
      s"${COLUMN_NAMES.BUCKET} = '$bucket'",
      s"${COLUMN_NAMES.SPAN} = ${timeFilter.span.index}"
    )

    // FIXME: Wrong logic here
    val geohashes = geoFilter.rect.geohashes(geoFilter.precision)
    if (geoFilter == WORLDWIDE) {
      conditions += s"${COLUMN_NAMES.GEOHASH} = ''"
    } else {
      val geohashConditions = geohashes.map(gh => s"'$gh'").mkString(", ")
      conditions += s"${COLUMN_NAMES.GEOHASH} IN ($geohashConditions)"
    }

    if (timeFilter == ALL_TIME) {
      conditions += s"${COLUMN_NAMES.TIMESTAMP} = 0"
    } else {
      conditions += s"${COLUMN_NAMES.TIMESTAMP} >= ${timeFilter.fromTime}"
      conditions += s"${COLUMN_NAMES.TIMESTAMP} < ${timeFilter.toTime}"
    }
    conditions.mkString(" and ")
  }
}
