package almanac.kafka

import almanac.AlmanacSettings._
import almanac.model.Metric
import almanac.model.Metric.{Key, Value, metric}
import almanac.spark.SparkMetricChannel
import kafka.producer.KeyedMessage
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils._

class MetricKeyDecoder extends Decoder[Key] {
  override def fromBytes(bytes: Array[Byte]): Key = {
    metric.increment("from.kafka").key
  }
}

class MetricValueDecoder extends Decoder[Value] {
  override def fromBytes(bytes: Array[Byte]): Value = {
    metric.increment("from.kafka").value
  }
}

object KafkaChannel extends SparkMetricChannel {
  private lazy val producer = new KafkaProducer[Key, Value](KafkaProperties)

  def stream(ssc: StreamingContext): DStream[Metric] = {
    // TODO: decoder and offset
    createDirectStream[String,        String,
                       StringDecoder, StringDecoder](ssc, KafkaConsumerParam, Set(KafkaMetricTopic)) map {
      case (k,v) =>
        // Metric(k, v)
        metric increment "from.kafka"
    }
  }

  override def send(metrics: Seq[Metric]): Unit = {
    metrics foreach { m =>
      val record = new ProducerRecord[Key, Value](KafkaMetricTopic, m.key, m.value)
      producer.send(record)
    }
  }
}
