package almanac.test

import almanac.AlmanacSettings._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{FunSuite, Matchers}

class KafkaSuite extends FunSuite with Matchers {
  val ssc = new StreamingContext(AlmanacSparkConf, Seconds(2))

  test("kafka string test") {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, KafkaConsumerParam, Set(KafkaMetricTopic)) print()

    ssc.start()
    ssc.awaitTermination()
  }
}
