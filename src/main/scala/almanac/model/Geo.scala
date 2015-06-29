package almanac.model

import almanac.model.Coordinate._
import almanac.model.GeoHash._

object GeoHash {
  type Bounds = (Double, Double)

  val LAT_RANGE = (-90.0, 90.0)
  val LNG_RANGE = (-180.0, 180.0)

  val WORLDWIDE = 0

  implicit class CoordinateWrapper(x: Double) {
    def in(b: Bounds): Boolean = x >= b._1 && x <= b._2
    def westOf(otherLng: Double): Boolean = if (x < otherLng) x + 180 > otherLng
                                            else if (x > 0 && otherLng <0) otherLng + 180 < x
                                            else false
    def eastOf(otherLng: Double): Boolean = !westOf(otherLng)
    def northOf(otherLat: Double): Boolean = otherLat < x
    def southOf(otherLat: Double): Boolean = !northOf(otherLat)
  }

  val BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

  def toBounds(geohash: String): (Bounds, Bounds) = {
    def charToInt(ch: Char) = BASE32.indexOf(ch)
    def intToBits(i: Int) = (4 to 0 by -1) map (i >> _ & 1) map (1==)
    def split[T](l: List[T]) = (l :\ (List[T](), List[T]())) { case (b, (list1, list2)) => (b :: list2, list1) }
    def dehash(xs: List[Boolean], bounds: Bounds): Bounds = (bounds /: xs ) {
      case ((min, max), bool) =>
        if (bool) ((min+max)/2, max)
        else      (min, (min+max)/2)
    }

    val (xs, ys) = split(geohash map charToInt flatMap intToBits toList)
    (dehash(ys, LAT_RANGE), dehash(xs, LNG_RANGE))
  }

  def decode(geohash: String): (Double, Double) = {
    toBounds(geohash) match {
      case ((minLat, maxLat), (minLng, maxLng)) => ( (maxLat+minLat)/2, (maxLng+minLng)/2 )
    }
  }

  private def interleave[T](list1: List[T], list2: List[T]): List[T] = (list1, list2) match {
    case (x, Nil) => x
    case (Nil, y) => y
    case (x :: xs, y :: ys) => x :: y :: interleave(xs, ys)
    case _ => Nil
  }

  private def toBits(magnitude: Double, bounds: Bounds, numBits: Int): List[Int] = {
    val (min, max) = bounds
    if (numBits == 0) Nil
    else {
      val mid = (min + max) / 2
      if (magnitude >= mid) 1 :: toBits(magnitude, (mid, max), numBits - 1)
      else                  0 :: toBits(magnitude, (min, mid), numBits - 1)
    }
  }

  def encode(lat: Double, lng: Double, precision: Int = 12) = {
    require(lat in LAT_RANGE, "Latitude out of range")
    require(lng in LNG_RANGE, "Longitude out of range")
    require(precision >= 0, "Precision must be a positive integer")
    val numBits = precision * 5 / 2
    val latBits = toBits(lat, LAT_RANGE, numBits)
    val lngBits = toBits(lng, LNG_RANGE, numBits + precision % 2)
    interleave(lngBits, latBits).grouped(5).map { x =>
      BASE32(x reduceLeft { (acc, b) => (acc << 1) + b})
    }.mkString
  }

  implicit class GeoHashHelper(geohash: String) {
    def round(precision: Int): String = {
      require(precision >= 0)
      if (precision <= geohash.size) geohash.substring(0, precision) else geohash
    }
    def ~(precision: Int) = round(precision)
  }
}

object Coordinate {
  implicit class CoordinateBuilder(latitude: Double) {

    /**
     * latitude intersect longitude, to build a coordinate
     * @param longitude the longitude to intersect with
     * @return a Coordinate of the included latitude and the passed longitude
     */
    def x(longitude: Double) = Coordinate(latitude, longitude)
    def lng(longitude: Double) = x(longitude)
  }

  def lat(latitude: Double) = new CoordinateBuilder(latitude)

  def apply(geohash: String): Coordinate = {
    val (lat, lng) = GeoHash.decode(geohash)
    lat x lng
  }
}

case class Coordinate(lat: Double, lng: Double) {
  require(lat in LAT_RANGE, "latitude out of bound [-90, 90]")
  require(lng in LNG_RANGE, "longitude out of bound [-180, 180]")

  lazy val geohash = encode(lat, lng)
}

object GeoRect {
  def apply(geohash: String): GeoRect = {
    val ((minLat, maxLat), (minLng, maxLng)) = toBounds(geohash)
    GeoRect(maxLat x maxLng, minLat x minLng)
  }

  def apply(latBounds: Bounds, lngBounds: Bounds): GeoRect = {
    val ((minLat, maxLat), (minLng, maxLng)) = (latBounds, lngBounds)
    GeoRect(maxLat x maxLng, minLat x minLng)
  }

  def apply(co1: Coordinate, co2: Coordinate) = new GeoRect(
    if (co1.lat northOf co2.lat) co1.lat else co2.lat,
    if (co1.lat eastOf  co2.lat) co1.lng else co2.lng,
    if (co1.lng southOf co2.lng) co1.lat else co2.lat,
    if (co1.lng westOf  co2.lng) co1.lng else co2.lng)
}

trait Shape {
  def intersects(other: GeoRect): Boolean
  def intersects(geohash: String): Boolean
  def contains(co: Coordinate): Boolean
  def contains(other: GeoRect): Boolean
  def contains(geohash: String): Boolean
}

case class GeoRect(north: Double, east: Double, south: Double, west: Double) extends Shape {
  require((north in LAT_RANGE) && (south in LAT_RANGE), "latitude out of bound [-90, 90]")
  require((east in LNG_RANGE) && (west in LNG_RANGE), "longitude out of bound [-180, 180]")
  require(north >= south, "latitude north < south.")

  def top    = north
  def bottom = south
  def left   = west
  def right  = east
  lazy val center = (north+south)/2 x (west+east)/2

  def intersects(other: GeoRect): Boolean = !(
    (east westOf other.west) || (east == other.west) ||
    (west eastOf other.east) || (west == other.east) ||
    (south northOf other.north) || (south == other.north) ||
    (north southOf other.south) || (north == other.south) ||
    contains(other)
  )
  def intersects(geohash: String): Boolean = intersects(GeoRect(geohash))
  def contains(co: Coordinate): Boolean = ((co.lng eastOf west) || (co.lng == west)) &&
                                          ((co.lng westOf east) || (co.lng == east)) &&
                                          ((co.lat southOf north) || (co.lat == north)) &&
                                          ((co.lat northOf south) || (co.lat == south))
  def contains(other: GeoRect): Boolean = contains(other.south x other.west) && contains(other.north x other.east)
  def contains(geohash: String): Boolean = contains(GeoRect(geohash))

  def geohashes(precision: Int, inner: Boolean = false): Set[String] = {
    def _geohashes(prefix: String): Seq[String] = {
      if (prefix.length >= precision)
        if (inner) Seq.empty else Seq(prefix)
      else
        BASE32 map (prefix + _) flatMap { gh =>
          if (this contains gh) Seq(gh)
          else if (this intersects gh) _geohashes(gh)
          else Seq.empty
        }
    }
    _geohashes("").toSet
  }

  def geohashesWithLimit(limit: Int, inner: Boolean = false): Set[String] = {
    ((Set.empty[String], true) /: (1 to 12)) { case ((ghs, continue), p) =>
      if (continue) {
        val newResult = geohashes(p, inner)
        if (newResult.size > limit) (ghs, false)
        else (newResult, true)
      } else {
        (ghs, continue)
      }
    }._1
  }

  def geohashesWithLowestPrecision(inner: Boolean = false): Set[String] = {
    var result = Set.empty[String]
    var precision = 0
    do {
      precision += 1
      result = geohashes(precision, inner)
    } while (result.size == 0)
    result
  }

}
