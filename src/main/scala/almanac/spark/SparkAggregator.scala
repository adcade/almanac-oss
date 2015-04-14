package almanac.spark

import almanac.model.Metric._
import almanac.model.TimeSpan._
import almanac.model.{Metric, TimeSpan}
import almanac.util.MetricsGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkAggregator {
  implicit class MetricsRDD(val rdd: RDD[Metric]) {
    def aggregateByTimeSpan(span: TimeSpan) = aggregate(_ | span)
    def aggregateByFacts(facts: String*) = aggregate(_ & facts)
//    def aggregateByBucket(regex: String) = aggregate(_.bucket.matches(regex))

    def aggregate(f: Key => Key) =
      rdd map (m => f(m.key) -> m.value) reduceByKey (_+_) map (t => Metric(t._1, t._2))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("almanac").setMaster("local")
    val sc = new SparkContext(conf)

    val metrics = MetricsGenerator.generateRaw(5000000)
    val result = (sc.parallelize(metrics) aggregateByTimeSpan DAY aggregateByFacts "device") collect()

    result foreach println
  }
}