import almanac.model.{Metric, TimeSpan}
import almanac.persist.CassandraRddDao
import almanac.util.MetricsGenerator
import com.datastax.spark.connector.SomeColumns
import java.util.UUID
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.scalatest.{Matchers, FunSuite}
import scala.collection.mutable
import scala.util.Random

/**
 * Created by binliu on 6/28/15.
 */
class CassandraRddDaoSuite extends FunSuite with Matchers{

  val conf = new SparkConf().setAppName("almanac")
    .set("spark.cassandra.connection.host", "localhost")
  val sc = new SparkContext("local[2]", "test", conf)
  val ssc = new StreamingContext(sc, Seconds(1))

  test("test handle") {
    val dao = new CassandraRddDao(sc)
    val q = mutable.Queue[RDD[Metric]]()
    for (i <- 1 to 10) { _: Int =>
      q.enqueue(sc.parallelize(MetricsGenerator.generateRawWithGeo(100)))
    }
    val stream = ssc.queueStream(q, true)
    dao.handle(TimeSpan.ALL_TIME, 1, stream)
    //ssc.awaitTermination()
  }

  test("test save") {
    val metrics = MetricsGenerator.generateRawWithGeo(100)
    val rdds = sc.parallelize(metrics)
    val dao = new CassandraRddDao(sc)
    dao.save(rdds)
    dao.read(metrics.head.bucket, metrics.head.geohash, metrics.head.span)
//    metrics.foreach { metric =>
//      val inDb = dao.read(metric.bucket, metric.geohash, metric.span)
//      inDb.foreach { m =>
//        println(m)
//      }
//      //println(inDb)
//    }
  }
}
