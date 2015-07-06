package almanac.test

import almanac.kafka._
import almanac.model.Coordinate
import almanac.model.Metric.withFacts
import kafka.utils.VerifiableProperties
import org.scalatest.{FunSuite, Matchers}

class SerializationSuite extends FunSuite with Matchers {
  test("kafka serializer test") {
    val metric = withFacts("class" -> "uberx") locate Coordinate("dr5ru7z77") increment "trip.update"
    val kser = new MetricKeySerializer(new VerifiableProperties())
    val vser = new MetricValueSerializer(new VerifiableProperties())

    kser.fromBytes(kser.toBytes(metric.key)) shouldBe metric.key
    vser.fromBytes(vser.toBytes(metric.value)) shouldBe metric.value
  }
}
