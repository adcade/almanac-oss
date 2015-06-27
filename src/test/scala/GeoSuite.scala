import almanac.model.{GeoHash, GeoRect, Coordinate}
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
    an[IllegalArgumentException] should be thrownBy Coordinate(-91, 0)
    an[IllegalArgumentException] should be thrownBy Coordinate(91, 0)
    an[IllegalArgumentException] should be thrownBy Coordinate(0, 181)
    an[IllegalArgumentException] should be thrownBy Coordinate(0, -181)

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

    assert((Coordinate(0,  -45) westOf   45) && !(Coordinate(0,  -45) eastOf   45))
    assert((Coordinate(0,   45) westOf  135) && !(Coordinate(0,   45) eastOf  135))
    assert((Coordinate(0,  135) westOf -135) && !(Coordinate(0,  135) eastOf -135))
    assert((Coordinate(0, -135) westOf  -45) && !(Coordinate(0, -135) eastOf  -45))

    assert((Coordinate(0,   45) eastOf  -45) && !(Coordinate(0,   45) westOf  -45))
    assert((Coordinate(0,  135) eastOf   45) && !(Coordinate(0,  135) westOf   45))
    assert((Coordinate(0, -135) eastOf  135) && !(Coordinate(0, -135) westOf  135))
    assert((Coordinate(0,  -45) eastOf -135) && !(Coordinate(0,  -45) westOf -135))
  }


  test("latitude 45 northOf 15 northOf -15 northOf -45") {
    assert  (Coordinate( 45, 0) northOf  15)
    assert  (Coordinate( 15, 0) northOf -15)
    assert  (Coordinate(-15, 0) northOf -45)
    assert(!(Coordinate(-45, 0) northOf  45))
  }
}