package almanac.spark

import almanac.model.Metric._
import almanac.model.TimeSpan._
import almanac.model.{GeoPrecision, Metric, TimeSpan}
import almanac.util.MetricsGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//import com.datastax.spark.connector._

object SparkMetricsAggregator {
  implicit class MetricsRDD(val rdd: RDD[Metric]) {
    def aggregateByTimeSpan(span: TimeSpan) = aggregate(_ ~ span)
    def aggregateByFacts(facts: String*) = aggregate(_ & facts)
    def aggregateByGeoPrecision(precision: GeoPrecision) = aggregate(_ ~ precision)
//    def aggregateByBucket(regex: String) = aggregate(_.bucket.matches(regex))

    def aggregate(func: Key => Key) =
      rdd map (m => func(m.key) -> m.value) reduceByKey (_+_) map (t => Metric(t._1, t._2))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("almanac")
    val sc = new SparkContext("local", "test", conf)

    val metrics = MetricsGenerator.generateRaw(5000000)
    val result = (sc.parallelize(metrics) aggregateByTimeSpan DAY aggregateByFacts "device") collect()

    result foreach println
  }
}