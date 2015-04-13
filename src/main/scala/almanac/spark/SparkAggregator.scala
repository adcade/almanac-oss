package almanac.spark

import almanac.model.Metric._
import almanac.model.TimeSpan._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

class SparkAggregator {

}

object SparkAggregator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("almanac").setMaster("local")
    val sc = new SparkContext(conf)

    val facts = Seq(withFacts("f1" -> "a", "f2" -> "1"),
                    withFacts("f1" -> "b", "f2" -> "2"),
                    withFacts("f3" -> "c", "f2" -> "3"))
    val ranGen = new Random
    val metrics = 1 to 1000 map (n => facts(ranGen nextInt facts.size) increment "some.counter")

    val intermediaRDD = sc.parallelize(metrics) map (m => (m.key | MONTH) -> m.value) reduceByKey (_ + _)

    intermediaRDD.cache()

    val result = intermediaRDD.collect()

    println(result)
  }
}