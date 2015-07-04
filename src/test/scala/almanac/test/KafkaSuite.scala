package almanac.test

import almanac.AlmanacSettings._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{FunSuite, Matchers}

class KafkaSuite extends FunSuite with Matchers {
  val ssc = new StreamingContext(AlmanacSparkConf, Seconds(2))
  val kafkaParams = Map[String, String]("metadata.broker.list" -> KafkaBrokers)

  test("kafka string test") {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(KafkaMetricTopic)).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
