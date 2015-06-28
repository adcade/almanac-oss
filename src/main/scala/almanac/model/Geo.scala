package almanac.model

object GeoHash {
  type Bounds = (Double, Double)

  val LAT_RANGE = (-90.0, 90.0)
  val LNG_RANGE = (-180.0, 180.0)

  val WORLDWIDE = 0

  implicit class BoundedNum(x: Double) {
    def in(b: Bounds): Boolean = x >= b._1 && x <= b._2
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
  def apply(geohash: String): Coordinate = {
    val (lat, lng) = GeoHash.decode(geohash)
    Coordinate(lat, lng)
  }
}

case class Coordinate(lat: Double, lng: Double) {
  import GeoHash._
  require(lat in LAT_RANGE, "latitude out of bound [-90, 90]")
  require(lng in LNG_RANGE, "longitude out of bound [-180, 180]")

  def westOf(otherLng: Double): Boolean = if (lng <= otherLng) lng + 180 > otherLng
                                          else if (lng > 0 && otherLng <0) otherLng + 180 < lng
                                          else false
  def eastOf(otherLng: Double): Boolean = !westOf(otherLng)
  def northOf(otherLat: Double): Boolean = otherLat <= lat
  def southOf(otherLat: Double): Boolean = !northOf(otherLat)

  def eastOf(otherCo: Coordinate): Boolean = eastOf(otherCo.lng)
  def westOf(otherCo: Coordinate): Boolean = westOf(otherCo.lng)
  def northOf(otherCo: Coordinate): Boolean = northOf(otherCo.lat)
  def southOf(otherCo: Coordinate): Boolean = southOf(otherCo.lat)

  lazy val geohash = encode(lat, lng)
}

object GeoRect {
  import GeoHash._

  def apply(geohash: String): GeoRect = {
    toBounds(geohash) match {
      case ((minLat, maxLat), (minLng, maxLng)) => GeoRect(Coordinate(maxLat, maxLng), Coordinate(minLat, minLng))
    }
  }

  def apply(co1: Coordinate, co2: Coordinate) = new GeoRect(
    if (co1 northOf co2) co1.lat else co2.lat,
    if (co1 eastOf  co2) co1.lng else co2.lng,
    if (co1 southOf co2) co1.lat else co2.lat,
    if (co1 westOf  co2) co1.lng else co2.lng)
}

case class GeoRect(north: Double, east: Double, south: Double, west: Double) {
  import GeoHash._
  require((north in LAT_RANGE) && (south in LAT_RANGE), "latitude out of bound [-90, 90]")
  require((east in LNG_RANGE) && (west in LNG_RANGE), "longitude out of bound [-180, 180]")
  require(north >= south, "latitude north < south.")

  def top    = north
  def bottom = south
  def left   = west
  def right  = east
  lazy val center = Coordinate((north+south)/2, (west+east)/2)

  def contains(co: Coordinate): Boolean = (co eastOf west) && (co westOf east) && (co southOf north) && (co northOf south)
  def contains(geohash: String): Boolean = {
    val ((minLat, maxLat), (minLng, maxLng)) = toBounds(geohash)
    contains(Coordinate(minLat, minLng)) && contains(Coordinate(maxLat, maxLng))
  }

  def geohashes(precision: Int): Set[String] = ???
//  def geohashes(precision: Int): Set[String] = {
//    def geohashes(geohash: String) = {
//      BASE32.
//    }
//  }

}
