package almanac.kafka

import almanac.AlmanacSettings._
import almanac.api.{MetricSinkFactory, MetricSink}
import almanac.model.Metric
import almanac.model.Metric.{Key, Value}
import almanac.spark.{DStreamSourceFactory, DStreamSource}
import com.twitter.chill.ScalaKryoInstantiator.defaultPool
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils._

class MetricKeyEncoder(verifiableProperties: VerifiableProperties) extends Encoder[Key] {
  override def toBytes(t: Key): Array[Byte] = defaultPool.toBytesWithClass(t)
}

class MetricValueEncoder(verifiableProperties: VerifiableProperties) extends Encoder[Value] {
  override def toBytes(t: Value): Array[Byte] = defaultPool.toBytesWithClass(t)
}

class MetricKeyDecoder(verifiableProperties: VerifiableProperties) extends Decoder[Key] {
  override def fromBytes(bytes: Array[Byte]): Key = defaultPool.fromBytes(bytes).asInstanceOf[Key]
}

class MetricValueDecoder(verifiableProperties: VerifiableProperties) extends Decoder[Value] {
  override def fromBytes(bytes: Array[Byte]): Value = defaultPool.fromBytes(bytes).asInstanceOf[Value]
}



class KafkaChannel extends MetricSink with DStreamSource[Metric] {
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

  override def close() = producer.close()
}

object KafkaChannelFactory extends MetricSinkFactory with DStreamSourceFactory[Metric] {
  override def createSink: KafkaChannel = new KafkaChannel()
  override def createSource: KafkaChannel = new KafkaChannel()
}

