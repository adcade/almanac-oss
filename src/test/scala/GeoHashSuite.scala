import almanac.model.{Coordinate, GeoHash}
import org.scalatest.{FunSuite, Matchers}

class GeoHashSuite extends FunSuite with Matchers {
  import GeoHash._

  test("decoding geohash to coordinate") {
    val (lat,lng) = decode("dr5ru1pcr6gu")
    lat should be ( 40.743028 +- 0.00001)
    lng should be (-73.992947 +- 0.00001)
  }

  test("encoding coordinate to geohash") {
    val geohash = "dr5ru1pcr6gu"
    val (lat,lng) = decode(geohash)
    encode(lat,lng) should equal (geohash)
    encode(lat,lng, 0) should equal ("")
  }

  test("decodeBound should return ?") {
    val ((lat1,lng1), (lat2, lng2)) = decodeBounds("dr5r")
//    lat1 should be ( 40.743028 +- 0.00001)
//    lng1 should be (-73.992947 +- 0.00001)
//    lat2 should be ( 40.743028 +- 0.00001)
//    lng2 should be (-73.992947 +- 0.00001)
  }

  test("coordinate constructors and validators") {
    an[IllegalArgumentException] should be thrownBy Coordinate(-91, 0)
    an[IllegalArgumentException] should be thrownBy Coordinate(91, 0)
    an[IllegalArgumentException] should be thrownBy Coordinate(0, 181)
    an[IllegalArgumentException] should be thrownBy Coordinate(0, -181)

    val co = Coordinate("dr5ru1pcr6gu")
    co.lat should be ( 40.743028 +- 0.00001)
    co.lng should be (-73.992947 +- 0.00001)
  }

  test("longitude -45 westOf 45 westOf 135 westOf -135 westOf -45") {
    /*
    -135 180 135
    -90   N   90
      -45 0 45
    */

    assert  (Coordinate(0,  -45) westOf   45)
    assert  (Coordinate(0,   45) eastOf  -45)
    assert(!(Coordinate(0,   45) westOf  -45))
    assert(!(Coordinate(0,  -45) eastOf   45))

    assert  (Coordinate(0,   45) westOf  135)
    assert  (Coordinate(0,  135) eastOf   45)
    assert(!(Coordinate(0,  135) westOf   45))
    assert(!(Coordinate(0,   45) eastOf  135))

    assert  (Coordinate(0,  135) westOf -135)
    assert  (Coordinate(0, -135) eastOf  135)
    assert(!(Coordinate(0, -135) westOf  135))
    assert(!(Coordinate(0,  135) eastOf -135))

    assert  (Coordinate(0, -135) westOf  -45)
    assert  (Coordinate(0,  -45) eastOf -135)
    assert(!(Coordinate(0,  -45) westOf -135))
    assert(!(Coordinate(0, -135) eastOf  -45))
  }


  test("latitude 45 northOf 15 northOf -15 northOf -45") {
    assert  (Coordinate( 45, 0) northOf  15)
    assert  (Coordinate( 15, 0) northOf -15)
    assert  (Coordinate(-15, 0) northOf -45)
    assert(!(Coordinate(-45, 0) northOf  45))
  }
}