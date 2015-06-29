package almanac.test

import almanac.model.Coordinate._
import almanac.model.{Coordinate, GeoHash, GeoRect}
import org.scalatest.{FunSuite, Matchers}

import scala.util.Random

class GeoSuite extends FunSuite with Matchers {
  import GeoHash._

  test("decoding geohash to coordinate") {
    val (lat,lng) = decode("dr5ru1pcr6gu")
    lat should be ( 40.743028 +- 0.00001)
    lng should be (-73.992947 +- 0.00001)
  }

  test("encoding coordinate to geohash") {
    val geohash = "dr5ru1pcr6gu"
    val (lat,lng) = decode(geohash)
    encode(lat, lng) should equal (geohash)
    encode(lat, lng, 0) should equal ("")
  }

  test("decodeBound should return ?") {
    val geohash = "dr5ru1pcr6gu"
    val ((lat1,lng1), (lat2, lng2)) = toBounds(geohash)
    (12 to 0 by -1) foreach {x => println( (x, GeoRect(geohash ~ x)) )}
//    lat1 should be ( 40.743028 +- 0.00001)
//    lng1 should be (-73.992947 +- 0.00001)
//    lat2 should be ( 40.743028 +- 0.00001)
//    lng2 should be (-73.992947 +- 0.00001)
  }

  test("geohash round to any precision") {
    ("dr5ru1pcr6gu" ~ 14) should be ("dr5ru1pcr6gu")
    ("dr5ru1pcr6gu" ~ 8) should be ("dr5ru1pc")
    ("dr5ru1pcr6gu" ~ 2) should be ("dr")
    ("dr5ru1pcr6gu" ~ 0) should be ("")
  }

  test("coordinate constructors and validators") {
    an[IllegalArgumentException] should be thrownBy (-91.0 x 0.0)
    an[IllegalArgumentException] should be thrownBy (91.0 x 0.0)
    an[IllegalArgumentException] should be thrownBy (0.0 x 181.0)
    an[IllegalArgumentException] should be thrownBy (0.0 x -181.0)

    val geohash = "dr5ru1pcr6gu"
    val co = Coordinate("dr5ru1pcr6gu")
    co.geohash should be (geohash)
    co.lat should be ( 40.743028 +- 0.00001)
    co.lng should be (-73.992947 +- 0.00001)
  }

  test("longitude -45 westOf 45 westOf 135 westOf -135 westOf -45") {
    /*
    -135 180 135
    -90   N   90
      -45 0 45
    */

    assert( ( -45.0 westOf   45.0) && (  45.0 eastOf  -45.0) && !( -45.0 eastOf   45.0) && !(  45.0 westOf  -45.0) )
    assert( (  45.0 westOf  135.0) && ( 135.0 eastOf   45.0) && !(  45.0 eastOf  135.0) && !( 135.0 westOf   45.0) )
    assert( ( 135.0 westOf -135.0) && (-135.0 eastOf  135.0) && !( 135.0 eastOf -135.0) && !(-135.0 westOf  135.0) )
    assert( (-135.0 westOf  -45.0) && ( -45.0 eastOf -135.0) && !(-135.0 eastOf  -45.0) && !( -45.0 westOf -135.0) )
  }

  test("latitude 45 northOf 15 northOf -15 northOf -45") {
    assert  ( 45.0 northOf  15.0)
    assert  ( 15.0 northOf -15.0)
    assert  (-15.0 northOf -45.0)
    assert(!(-45.0 northOf  45.0))
  }

  test("rect intersect rect") {
    assert (GeoRect(10, 10, -10, -10) intersects GeoRect(20, 20, 0, 0))
    assert (!(GeoRect(10, 10, -10, -10) intersects GeoRect(20, 10, 10, -10)))
    assert (GeoRect(10, 10, -10, -10) intersects GeoRect(45, 10, -45, -10))
    assert (GeoRect(10, -170, -10, 170) intersects GeoRect(5, -160, -5, 160))
    assert (GeoRect(10, -170, -10, 170) intersects GeoRect(5, 180, -5, 160))
  }

  test("get geohashes of a GeoRect") {
    val rect1 = GeoRect("dr5ru1pcr6gu")
    rect1.geohashes(12) should be (Set("dr5ru1pcr6gu"))

    val rect2 = GeoRect("dr5ru")
    rect2.geohashes(12) should be (Set("dr5ru"))

    val rect3 = GeoRect(10.001, 10.001, 10, 10)
    rect3.geohashes(8) should be (Set("s1z0gsd1", "s1z0gsd7", "s1z0gsd2", "s1z0gs6p", "s1z0gsd6",
                                      "s1z0gsd3", "s1z0gsd5", "s1z0gsd4", "s1z0gs6r", "s1z0gsd0"))

    rect3.geohashes(7, inner=true) should be (Set())
    rect3.geohashes(7, inner=false) should be (Set("s1z0gs3", "s1z0gs6", "s1z0gs9", "s1z0gsd"))
  }

  test ("geohash experiment") {
    val geohash = "dr5ru1pcr6gu"

    for (p <- 0 to 8) {
      val rect = GeoRect(geohash.substring(0, p))
      println(geohash.substring(0, p), (rect.top - rect.bottom) * (rect.left - rect.right))
    }

    for {pLat <- 0 to 5
         pLng <- 0 to 5
         pGeohash <- 1 to 8
    } {
      val dLat = 1 * math.pow(0.1, pLat)
      val dLng = 1 * math.pow(0.1, pLng)
      val rect = GeoRect(dLat, dLng, 0, 0)
      val pLog = math.log10(dLat * dLng)
      val geohashes = rect.geohashes(pGeohash)
      if (geohashes.size > 0 && geohashes.size < 100)
        println(pLat, pLng, -math.round(pLog), pGeohash, geohashes.size)
    }
  }

  test ("geohash experiment2") {
    val ran = new Random
    def random(b: Bounds) = ran.nextDouble * (b._2 - b._1) + b._1
    var max = 0
    for (i <- 0 to 10000) {
      val rect = GeoRect(random(LAT_RANGE) x random(LNG_RANGE), random(LAT_RANGE) x random(LNG_RANGE))
//      val geohashes = rect.geohashesWithLimit(100)
      val geohashes = rect.geohashesWithLowestPrecision()
      if (geohashes.size > 1) {
        if (geohashes.size > max) max = geohashes.size
        println(geohashes.size, geohashes)
      }
    }

    println(max)

//    println(GeoRect(48.93840015820638,-138.875716813862,48.93840015820638,-138.875716813862).geohashes(12))
//    GeoRect(48.93840015820638,-138.875716813862,48.93840015820638,-138.875716813862)0
//    GeoRect(56.975747123881604,148.50458245836478,-45.28166729512666,65.14182092940422)64
//    GeoRect(6.887932568316444,43.17930519848326,6.887932568316444,43.17930519848326)0
//    GeoRect(58.19985902552139,-113.13142544147499,58.19985902552139,-113.13142544147499)0
//    GeoRect(45.291487177945385,164.60501299708545,21.182342120963654,164.60501299708545)
  }
}