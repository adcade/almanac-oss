package almanac.persist

import java.util.UUID

import almanac.model.{Metric, MetricsQuery, TimeSpan}
import almanac.spark.{MetricStreamHandler, MetricStreamRetriever}
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by binliu on 6/28/15.
 */
class CassandraRddDao(sc: SparkContext) extends MetricStreamHandler with MetricStreamRetriever with Serializable{

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
    val TIMESTAMP = "ts"
    val TOTAL = "total"
  }

  override def handle(span: TimeSpan, precision: Int, stream: DStream[Metric]): Unit = {
    stream.foreachRDD {rdd =>
      save(rdd)
    }
  }

  def save(rdd: RDD[Metric]) = {
    val mapped = rdd.map { m =>
      (m.bucket,
        m.count,
        UUID.randomUUID().toString,
        Map.empty,
        m.geohash,
        1L,
        0L,
        TimeSpan.values.indexOf(m.span),
        m.timestamp,
        1L)
    }
    mapped.saveToCassandra(KEYSPACE,
        TABLE_NAME,
        SomeColumns(
          COLUMN_NAMES.BUCKET,
          COLUMN_NAMES.COUNT,
          COLUMN_NAMES.FACTKEY,
          COLUMN_NAMES.FACTS,
          COLUMN_NAMES.GEOHASH,
          COLUMN_NAMES.MAX,
          COLUMN_NAMES.MIN,
          COLUMN_NAMES.SPAN,
          COLUMN_NAMES.TIMESTAMP,
          COLUMN_NAMES.TOTAL
        )
      )
  }

  def read(bucket: String, geoHash: String, span: TimeSpan) : Unit = {
    val queryAsCql = s" ${COLUMN_NAMES.BUCKET} = '${bucket}' and ${COLUMN_NAMES.GEOHASH} = '${geoHash}' and ${COLUMN_NAMES.SPAN} = ${span.index} "

    sc.cassandraTable(KEYSPACE, TABLE_NAME)
      .select(COLUMN_NAMES.BUCKET,
        COLUMN_NAMES.COUNT,
        COLUMN_NAMES.FACTKEY,
        COLUMN_NAMES.FACTS,
        COLUMN_NAMES.GEOHASH,
        COLUMN_NAMES.MAX,
        COLUMN_NAMES.MIN,
        COLUMN_NAMES.SPAN,
        COLUMN_NAMES.TIMESTAMP,
        COLUMN_NAMES.TOTAL)
      .where(queryAsCql)
      .map { r =>
      new Metric(
        bucket = r.getString(COLUMN_NAMES.BUCKET),
        facts = Map.empty,
        span = TimeSpan.values(r.getInt(COLUMN_NAMES.SPAN)),
        timestamp = r.getDate(COLUMN_NAMES.TIMESTAMP).getTime,
        geohash = r.getString(COLUMN_NAMES.GEOHASH),
        count = r.getInt(COLUMN_NAMES.COUNT),
        total = r.getLong(COLUMN_NAMES.TOTAL),
        max = r.getLong(COLUMN_NAMES.MAX),
        min = r.getLong(COLUMN_NAMES.MIN)
      )
    }
  }

  def read(query: MetricsQuery): List[Metric] = {
    val queryAsCql = s"${COLUMN_NAMES.BUCKET} = ${query.buckets.head} and ${COLUMN_NAMES.GEOHASH} = ${query.geoFilter.rect.geohashes(8).head} and ${COLUMN_NAMES.SPAN} = ${query.timeFilter.span}"

    sc.cassandraTable(KEYSPACE, TABLE_NAME)
      .select(COLUMN_NAMES.BUCKET,
        COLUMN_NAMES.COUNT,
        COLUMN_NAMES.FACTKEY,
        COLUMN_NAMES.FACTS,
        COLUMN_NAMES.GEOHASH,
        COLUMN_NAMES.MAX,
        COLUMN_NAMES.MIN,
        COLUMN_NAMES.SPAN,
        COLUMN_NAMES.TIMESTAMP,
        COLUMN_NAMES.TOTAL)
      .where(queryAsCql)
      .collect()
      .map { r =>
      new Metric(
        bucket = r.getString(COLUMN_NAMES.BUCKET),
        facts = Map.empty,
        span = TimeSpan.values(r.getInt(COLUMN_NAMES.SPAN)),
        timestamp = r.getDate(COLUMN_NAMES.TIMESTAMP).getTime,
        geohash = r.getString(COLUMN_NAMES.GEOHASH),
        count = r.getInt(COLUMN_NAMES.COUNT),
        total = r.getLong(COLUMN_NAMES.TOTAL),
        max = r.getLong(COLUMN_NAMES.MAX),
        min = r.getLong(COLUMN_NAMES.MIN)
      )
    }.toList
  }
}
