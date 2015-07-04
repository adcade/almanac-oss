package almanac.kafka

import almanac.AlmanacSettings._
import almanac.model.Metric
import almanac.model.Metric.{Key, Value, metric}
import almanac.spark.SparkMetricChannel
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils._

class MetricKeyEncoder(verifiableProperties: VerifiableProperties) extends Encoder[Key] {
  override def toBytes(t: Key): Array[Byte] = {
    "hello" getBytes "UTF8"
  }
}

class MetricValueEncoder(verifiableProperties: VerifiableProperties) extends Encoder[Value] {
  override def toBytes(t: Value): Array[Byte] = {
    "hello" getBytes "UTF8"
  }
}

class MetricKeyDecoder(verifiableProperties: VerifiableProperties) extends Decoder[Key] {
  override def fromBytes(bytes: Array[Byte]): Key = {
    metric.increment("from.kafka").key
  }
}

class MetricValueDecoder(verifiableProperties: VerifiableProperties) extends Decoder[Value] {
  override def fromBytes(bytes: Array[Byte]): Value = {
    metric.increment("from.kafka").value
  }
}

object KafkaChannel extends SparkMetricChannel {
  private lazy val producer = new Producer[Key, Value](KafkaProducerConfig)

  def stream(ssc: StreamingContext): DStream[Metric] = {
    // TODO: decoder and offset
    createDirectStream [Key, Value, MetricKeyDecoder, MetricValueDecoder] (ssc,
      kafkaParams = KafkaConsumerParam,
      topics = Set(KafkaMetricTopic)) map { case (k,v) => Metric(k, v) }
  }

  override def send(metrics: Seq[Metric]): Unit = {
    metrics foreach { m =>
      producer.send(new KeyedMessage[Key, Value](KafkaMetricTopic, m.key, m.value))
    }
  }
}
