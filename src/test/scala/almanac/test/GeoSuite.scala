package almanac.test

import almanac.model.Coordinate._
import almanac.model.{Coordinate, GeoHash, GeoRect}
import org.scalatest.{FunSuite, Matchers}

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

  test("toBounds should return ?") {
    toBounds("g") should be ((45.0, 90.0), (-45.0, 0))
    GeoRect("g") should be (GeoRect(90, 0, 45, -45))
    toBounds("") should be ((-90, 90), (-180, 180))
    GeoRect("") should be (GeoRect(90, 180, -90, -180))
    val geohash = "dr5ru1pcr6gu"
    val ((lat1,lng1), (lat2, lng2)) = toBounds(geohash)
    GeoRect(geohash ~ 0) should be (GeoRect(90, 180, -90, -180))
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
    assert (   GeoRect(90.0,180.0,-90.0,-180.0) intersects GeoRect("g")                    )
    assert (   GeoRect(90.0,180.0,-90.0,-180.0) intersects GeoRect(10, 10, -10, -10)       )
    assert (   GeoRect(90.0,180.0,-90.0,-180.0) intersects GeoRect(10, -170, -10, 170)     )
    assert (   GeoRect(10, 10, -10, -10)        intersects GeoRect(90.0,180.0,-90.0,-180.0))
    assert (   GeoRect(10, -170, -10, 170)      intersects GeoRect(90.0,180.0,-90.0,-180.0))
    assert (   GeoRect(10, 10, -10, -10)        intersects GeoRect(20, 20, 0, 0)           )
    assert ( !(GeoRect(10, 10, -10, -10)        intersects GeoRect(20, 10, 10, -10)       ))
    assert (   GeoRect(10, 10, -10, -10)        intersects GeoRect(45, 10, -45, -10)       )
    assert (   GeoRect(10, -170, -10, 170)      intersects GeoRect(5, -160, -5, 160)       )
    assert (   GeoRect(10, -170, -10, 170)      intersects GeoRect(5, 180, -5, 160)        )
  }

//  test("get geohashes of a GeoRect") {
//    val rect1 = GeoRect("dr5ru1pcr6gu")
//    rect1.geohashesToPricision(12) should be (Set("dr5ru1pcr6gu"))
//
//    val rect2 = GeoRect("dr5ru")
//    rect2.geohashesToPricision(12) should be (Set("dr5ru"))
//
//    val rect3 = GeoRect(10.001, 10.001, 10, 10)
//    rect3.geohashesToPricision(8) should be (Set("s1z0gsd1", "s1z0gsd7", "s1z0gsd2", "s1z0gs6p", "s1z0gsd6",
//                                      "s1z0gsd3", "s1z0gsd5", "s1z0gsd4", "s1z0gs6r", "s1z0gsd0"))
//
//    rect3.geohashesToPricision(7, inner=true) should be (Set())
//    rect3.geohashesToPricision(7, inner=false) should be (Set("s1z0gs3", "s1z0gs6", "s1z0gs9", "s1z0gsd"))
//  }
//
//  test ("geohash experiment") {
//    val geohash = "dr5ru1pcr6gu"
//
//    for (p <- 0 to 8) {
//      val rect = GeoRect(geohash.substring(0, p))
//      println(geohash.substring(0, p), (rect.top - rect.bottom) * (rect.left - rect.right))
//    }
//
//    for {pLat <- 0 to 5
//         pLng <- 0 to 5
//         pGeohash <- 1 to 8
//    } {
//      val dLat = 1 * math.pow(0.1, pLat)
//      val dLng = 1 * math.pow(0.1, pLng)
//      val rect = GeoRect(dLat, dLng, 0, 0)
//      val pLog = math.log10(dLat * dLng)
//      val geohashes = rect.geohashesToPricision(pGeohash)
//      if (geohashes.size > 0 && geohashes.size < 100)
//        println(pLat, pLng, -math.round(pLog), pGeohash, geohashes.size)
//    }
//  }
//
//  test ("geohash experiment2") {
//    val ran = new Random
//    def random(b: Bounds) = ran.nextDouble * (b._2 - b._1) + b._1
//    var max = 0
//    for (i <- 0 to 10000) {
//      val rect = GeoRect(random(LAT_RANGE) x random(LNG_RANGE), random(LAT_RANGE) x random(LNG_RANGE))
////      val geohashes = rect.geohashesWithLimit(100)
//      val geohashes = rect.geohashesWithLowestPrecision()
//      if (geohashes.size > 1) {
//        if (geohashes.size > max) max = geohashes.size
//        println(geohashes.size, geohashes)
//      }
//    }
//
//    println(max)
//  }

  test ("super-geohash and sub-geohashes") {
    superGeohash("gehs") should be (Some("gehs"))
    superGeohash("gehs", Set(7, 5, 2, 0)) should be (Some("ge"))
    superGeohash("g", Set(7, 5, 2)) should be (None)
    superGeohash("g", Set(7, 5, 2, 0)) should be (Some(""))

    subGeohashes("gehs") should be (Seq("gehs"))
    subGeohashes("gehs", Set(7, 5, 2, 0)).toSet should be ((for (i <- BASE32) yield s"gehs$i") toSet)
    subGeohashes("gehs", Set(2, 0)) should be (Nil)
    subGeohashes("", Set(2)).toSet should be ((for (i <- BASE32; j <- BASE32) yield s"$i$j") toSet)
  }

  test ("geohashes test") {
    GeoRect("c4").geohashes(Set.empty[Int]) should be (Set())
    GeoRect("c4").geohashes(Set(0)) should be (Set(""))
    GeoRect("gehs").geohashes(Set(7, 5, 2, 0)) should be ((for (i <- BASE32) yield s"gehs$i") toSet)
    GeoRect("c4").geohashes(Set(1)) should be (Set("c"))
    GeoRect("c4").geohashes(Set(1, 2, 4)) should be (Set("c4"))

    val rect = GeoRect(40.760650634765625, -73.98056030273438, 40.752410888671875, -73.99429321289062)
    rect.latMiddle x rect.lngMiddle

    val set_6 = Set(
      "dr5ruh", "dr5ruk", "dr5rus",
      "dr5ru5", "dr5ru7", "dr5rue",
      "dr5ru4", "dr5ru6", "dr5rud"
    )

    val set_7_6 = Set(
      "dr5ruhp", "dr5ruk0", "dr5ruk1", "dr5ruk4", "dr5ruk5", "dr5rukh", "dr5rukj", "dr5rukn", "dr5rukp", "dr5rus0",
      "dr5ru5z",                                                                                         "dr5rueb",
      "dr5ru5x",                                         "dr5ru7",                                       "dr5rue8",
      "dr5ru5r",                                                                                         "dr5rue2",
      "dr5ru5p",                                                                                         "dr5rue0",
      "dr5ru4z", "dr5ru6b", "dr5ru6c", "dr5ru6f", "dr5ru6g", "dr5ru6u", "dr5ru6v", "dr5ru6y", "dr5ru6z", "dr5rudb"
    )
    rect.geohashes(Set(6)) should be (set_6)
    rect.geohashes(Set(6, 7)) should be (set_7_6)

  }
}