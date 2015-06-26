package almanac.model

object Coordinate {
  type Bounds = (Double, Double)

  val LAT_RANGE = (-90.0, 90.0)
  val LNG_RANGE = (-180.0, 180.0)

  implicit class BoundedNum(x: Double) {
    def in(b: Bounds): Boolean = x >= b._1 && x <= b._2
  }

  private val BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
  private def charToInt(ch: Char) = BASE32.indexOf(ch)
  private def intToBits(i: Int) = (4 to 0 by -1) map (i >> _ & 1) map (1==)
  private def split[T](l: List[T]) = (l :\ (List[T](), List[T]())) { case (b, (list1, list2)) => (b :: list2, list1) }
  private def dehash(xs: List[Boolean], bounds: Bounds): Bounds = (bounds /: xs ) {
    case ((min, max), bool) =>
      if (bool) ( (min+max)/2, max )
      else ( min, (min+max)/2 )
  }

  def decodeBounds(geohash: String): (Bounds, Bounds) = {
    val (xs, ys) = split(geohash map charToInt flatMap intToBits toList)
    (dehash(ys, LAT_RANGE), dehash(xs, LNG_RANGE))
  }

  def fromGeoHash(geohash: String): (Double, Double) = {
    decodeBounds(geohash) match {
      case ((minLat, maxLat), (minLng, maxLng)) => ( (maxLat+minLat)/2, (maxLng+minLng)/2 )
    }
  }

  def toGeoHash(lat: Double, lng: Double, precision: Int = 12) = {
    var (minLat,maxLat) = (-90.0,90.0)
    var (minLng,maxLng) = (-180.0,180.0)
    val bits = List(16,8,4,2,1)

    (0 until precision).map{ p => {
      BASE32 apply (0 until 5).map{ i => {
        if (((5 * p) + i) % 2 == 0) {
          val mid = (minLng+maxLng)/2.0
          if (lng > mid) {
            minLng = mid
            bits(i)
          } else {
            maxLng = mid
            0
          }
        } else {
          val mid = (minLat+maxLat)/2.0
          if (lat > mid) {
            minLat = mid
            bits(i)
          } else {
            maxLat = mid
            0
          }
        }
      }}.reduceLeft( (a,b) => a|b )
    }}.mkString("")
  }

  implicit class GeoHashHelper(geohash: String) {
    def round(precision: Int): String = {
      require(precision > 0)
      geohash.substring(0, precision)
    }
    def ~(precision: Int) = round(precision)
  }

  def apply(geohash: String): Coordinate = {
    val (lat, lng) = fromGeoHash(geohash)
    Coordinate(lat, lng)
  }
}

case class Coordinate(lat: Double, lng: Double) {
  import Coordinate._
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

  lazy val geohash = toGeoHash(lat, lng)
}

object GeoRect {
  def apply(co1: Coordinate, co2: Coordinate) = new GeoRect(
    if (co1 northOf co2) co1.lat else co2.lat,
    if (co1 eastOf  co2) co1.lng else co2.lng,
    if (co1 southOf co2) co1.lat else co2.lat,
    if (co1 westOf  co2) co1.lng else co2.lng)
}

case class GeoRect(north: Double, east: Double, south: Double, west: Double) {
  import Coordinate._
  require((north in LAT_RANGE) && (south in LAT_RANGE), "latitude out of bound [-90, 90]")
  require((east in LNG_RANGE) && (west in LNG_RANGE), "longitude out of bound [-180, 180]")
  require(north >= south, "latitude north < south.")

  def top    = north
  def bottom = south
  def left   = west
  def right  = east
  lazy val center = Coordinate((north+south)/2, (west+east)/2)

  def encompass(co: Coordinate) = (co eastOf west) && (co westOf east) && (co southOf north) && (co northOf south)
}
