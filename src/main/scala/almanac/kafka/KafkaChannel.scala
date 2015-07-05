package almanac.kafka

import java.util.Properties

import almanac.AlmanacSettings._
import almanac.api.{MetricSink, MetricSinkFactory}
import almanac.model.Metric
import almanac.model.Metric.{Key, Value}
import almanac.spark.{DStreamSource, DStreamSourceFactory}
import com.twitter.chill.ScalaKryoInstantiator.defaultPool
import kafka.admin.AdminUtils._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.{Decoder, Encoder}
import kafka.utils.{VerifiableProperties, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
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
  private lazy val producer = new Producer[Key, Value](new ProducerConfig(KafkaProducerProperties))
  val zkClient = ZookeeperUtils.createClient()

  createTopicIfNotExists(KafkaMetricTopic, KafkaMetricTopicPartitionNum, KafkaMetricTopicReplicationFactor)

  def createTopicIfNotExists(topic: String, partitionNum: Int = 1, replicationFactor: Int = 1,
                             config: Properties = new Properties()): Unit = {
    if (! topicExists(zkClient, topic)) {
      createTopic(zkClient, topic, partitionNum, replicationFactor, config)
      println("created")
    }
  }

  def stream(ssc: StreamingContext): DStream[Metric] = {
    // TODO: check offset?
    createDirectStream [Key, Value, MetricKeyDecoder, MetricValueDecoder] (ssc,
      kafkaParams = KafkaConsumerParam,
      topics = Set(KafkaMetricTopic)) map { case (k,v) => Metric(k, v) }
  }

  override def send(metrics: Seq[Metric]): Unit = {
    metrics foreach { m =>
      producer.send(new KeyedMessage[Key, Value](KafkaMetricTopic, m.key, m.value))
    }
  }

  override def close() = {
    zkClient.close()
    producer.close()
  }
}

object KafkaChannelFactory extends MetricSinkFactory with DStreamSourceFactory[Metric] {
  override def createSink: KafkaChannel = new KafkaChannel()
  override def createSource: KafkaChannel = new KafkaChannel()
}

object ZookeeperUtils {
  def createClient(config: Properties = KafkaProducerProperties,
                   sessTimeout: Int = 10000,
                   connTimeout: Int = 10000,
                   serializer: ZkSerializer = ZKStringSerializer): ZkClient = {
    val host = config.getProperty("zookeeper.connect")
    new ZkClient(host, sessTimeout, connTimeout, serializer)
  }
}
