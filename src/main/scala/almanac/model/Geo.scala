package almanac.model

import almanac.model.Coordinate._
import almanac.model.GeoHash._

object GeoHash {
  type Bounds = (Double, Double)

  val LAT_RANGE = (-90.0, 90.0)
  val LNG_RANGE = (-180.0, 180.0)

  val WORLDWIDE = 0
  val MAX_PRECISION = 12

  lazy val ALL_PRECISION = (0 to 12) toSet

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

  /**
   * find the near sub geohashes, could be the geohash itself if precision level exist
   *
   * @param geohash the original geohash
   * @param precisions a set of precisions the geohashes should ranged in
   * @return
   */
  def subGeohashes(geohash: String, precisions: Set[Int] = ALL_PRECISION): Seq[String] =
    if (precisions contains geohash.length) Seq(geohash)
    else if (geohash.length + 1 > precisions.max) Nil // no deeper search
    else BASE32 flatMap { char => subGeohashes(geohash + char, precisions) }

  /**
   * find the near super geohash, could be the geohash itself if precision level exist
   *
   * if none of the upper layer precision is present, return None
   *
   * @param geohash the original geohash
   * @param precisions a set of precisions the geohashes should ranged in
   * @return Optional superGeohash result
   */
  def superGeohash(geohash: String, precisions: Set[Int] = ALL_PRECISION): Option[String] =
    if (precisions contains geohash.length) Some(geohash)
    else if (geohash.isEmpty) None // you can't have super geohash of empty String
    else superGeohash(geohash.substring(0, geohash.length - 1), precisions)

  def toBounds(geohash: String): (Bounds, Bounds) = {
    def charToInt(ch: Char) = BASE32.indexOf(ch)
    def intToBits(i: Int) = (4 to 0 by -1) map (i >> _ & 1) map (1==)
    def split[T](l: List[T]) = (l :\ (List[T](), List[T]())) { case (b, (list1, list2)) => (b :: list2, list1) }
    def fromBits(bits: List[Boolean], bounds: Bounds): Bounds = (bounds /: bits ) {
      case ((min, max), bool) =>
        if (bool) ((min+max)/2, max)
        else      (min, (min+max)/2)
    }

    val (lngBits, latBits) = split(geohash map charToInt flatMap intToBits toList)
    (fromBits(latBits, LAT_RANGE), fromBits(lngBits, LNG_RANGE))
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
      if (precision <= geohash.length) geohash.substring(0, precision) else geohash
    }
    def ~(precision: Int) = round(precision)
  }
}

object Coordinate {
  implicit class CoordinateBuilder(latitude: Double) {

    /**
     * latitude intersect longitude, to build a coordinate
     * let's say you have:
     * <code>
     *  val lat = 3.0;
     *  val lng = 4.0
     * </code>
     * then
     * <code>lat x lng</code> will return you a
     * <code>Coordinate(lat, lng)</code>
     *
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
    GeoRect(maxLat, maxLng, minLat, minLng)
  }

  def apply(latBounds: Bounds, lngBounds: Bounds): GeoRect = {
    val ((minLat, maxLat), (minLng, maxLng)) = (latBounds, lngBounds)
    GeoRect(maxLat x maxLng, minLat x minLng)
  }

  /**
   * giving arbitary two coordinate and get the rectangle they form.
   * the coordinate given could be any of the following:
   *  top-left, top-right, bottom-left, bottom-right
   *
   * warning this class is convenient, but here is the caveats:
   *
   * It is ambious situation where the bigger rectangle is desired or the smaller one!
   * for example, give co1(45, 20) co2(30, -20) there is two possible rectangle:
   *
   *  co1(45, 20, 30, -20) and co2(45, -20, 30, -20)
   *
   * here this function will simply use the smaller rectangle,
   * that is the distance between left and right is always less than 180 degree
   *
   * @param co1 the first coordinate to make the geo rectangle
   * @param co2 the second coordinate to make the geo rectangle
   * @return a georectangle that exactly covers these two coordinate
   */
  def apply(co1: Coordinate, co2: Coordinate) = new GeoRect(
    if (co1.lat northOf co2.lat) co1.lat else co2.lat,
    if (co1.lat eastOf  co2.lat) co1.lng else co2.lng,
    if (co1.lng southOf co2.lng) co1.lat else co2.lat,
    if (co1.lng westOf  co1.lng) co1.lng else co2.lng)
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

  def latMiddle = (north + south) / 2
  def lngMiddle = {
    val middle = (east + west) / 2
    if (east >= west) middle
    else if (middle > 0) middle - 180
    else middle + 180
  }

  def topChild = GeoRect(north, east, latMiddle, west)
  def bottomChild = GeoRect(latMiddle, east, south, west)
  def leftChild = GeoRect(north, lngMiddle, south, west)
  def rightChild = GeoRect(north, east, south, lngMiddle)

  def latSpan = (south, north)
  def lngSpan = (math.min(east, west), math.max(east, west))

  private def latIntersect(bottom1: Double, top1: Double, bottom2: Double, top2: Double): Boolean =
    (bottom1 <= bottom2 && bottom2 <  top1) ||
    (bottom1 <  top2    && top2    <= top1) ||
    (bottom2 <= bottom1 && bottom1 <  top2) ||
    (bottom2 <  top1    && top1    <= top2)

  /**
   * This is different from latitude intersects mainly because of the cyclic natural of longitude
   *
   * if IDL is in between [l1, r1] (l1 r1 can't equals to IDL in the same time)
   *   if l2/r2 E (l1, IDL] U [IDL, r1] then they intersects
   * if IDL is in between [l2, r2]
   *   if l1/r1 E (l2, IDL] U [IDL, r2] then they intersects
   *
   * @param left1
   * @param right1
   * @param left2
   * @param right2
   * @return
   */
  private def lngIntersect(left1: Double, right1: Double, left2: Double, right2: Double): Boolean =
    (left1 <= left2  && left2  <  right1) || // l2 E (l1, r1]
    (left1 <  right2 && right2 <= right1) || // r2 E [l1, r1)
    (left2 <= left1  && left1  <  right2) || // l1 E (l2, r2]
    (left2 <  right1 && right1 <= right2) || // r2 E [l2, r1)
    (left1 > right1 && !(left1 == 180 && right1 == -180)) && (                  // if IDL E [l1, r1]
      (left1 <= left2  && left2  <= 180) || (-180 <= left2  && left2  <  right1) || // l2 E (l1, IDL] U [IDL, r1]
      (left1 <  right2 && right2 <= 180) || (-180 <= right2 && right2 <= right1)    // r2 E [l1, IDL] U [IDL, r1)
    ) ||
    (left2 > right2 && !(left2 == 180 && right2 == -180)) && (                  // if IDL E [l1, r1]
      (left2 <= left1  && left1  <= 180) || (-180 <= left1  && left1  <  right2) || // l1 E (l2, IDL] U [IDL, r2]
      (left2 <  right1 && right1 <= 180) || (-180 <= right1 && right1 <= right2)    // r1 E [l2, IDL] U [IDL, r2)
    )

  private def latContains(bottom1: Double, top1: Double, bottom2: Double, top2: Double): Boolean =
    bottom1 <= bottom2 && bottom2 < top1 && bottom1 < top2 && top2 <= top1 // b2 E [b1, t1) && t2 E (b1, t1]

  private def lngContains(left1: Double, right1: Double, left2: Double, right2: Double): Boolean =
    if (left1 <= right1) left1 <= left2 && left2 <= right2 && right2 <= right1            // [l1 -> l2 -> r2 -> r1]
    else (left1 <= left2  && left2  <= right2 && right2 <= 180    && -180   <  right1) || // [l1 -> l2 -> r2 -> IDL -> r1]
         (left1 <= left2  && left2  <= 180    && -180   <= right2 && right2 <= right1) || // [l1 -> l2 -> IDL -> r2 -> r1]
         (left1 <  180    && -180   <= left2  && left2  <= right2 && right2 <= right1)    // [l1 -> IDL -> l2 -> r2 -> r1]

  def intersects(other: GeoRect): Boolean =
    latIntersect(south, north, other.south, other.north) &&
    lngIntersect(west, east, other.west, other.east)

  def intersects(geohash: String): Boolean = intersects(GeoRect(geohash))
  def contains(co: Coordinate): Boolean = ((co.lng eastOf west) || (co.lng == west)) &&
                                          ((co.lng westOf east) || (co.lng == east)) &&
                                          ((co.lat southOf north) || (co.lat == north)) &&
                                          ((co.lat northOf south) || (co.lat == south))
  def contains(other: GeoRect): Boolean =
    latContains(south, north, other.south, other.north) &&
    lngContains(west, east, other.west, other.east)

  def contains(geohash: String): Boolean = contains(GeoRect(geohash))

  /**
   * return geohashes of this GeoRect on only given precision level
   *
   * Example:
   * if pass in Set(0, 2, 4, 6)
   *
   * returns geohashes 0
   *
   * @param precisions a set of precisions the geohashes should ranged in
   * @return a set of geohashes
   */
  def geohashes(precisions: Set[Int] = ALL_PRECISION): Set[String] = {
    def next(gh: String): Seq[String] = {
      if (gh.length >= precisions.max) Seq(superGeohash(gh, precisions)).flatten
      else if (this contains gh) subGeohashes(gh, precisions)
      else level(gh, GeoRect(gh), 1, 0) flatMap next
    }

    def level(prefix:String, rect: GeoRect, bitmap: Int, even: Int): Seq[String] = {
      if (bitmap >= 32) Seq(prefix + BASE32(bitmap - 32))
      else Seq(0, 1) flatMap (bit => {
        val halfRect =
          if ((prefix.length * 5 + even) % 2 == 0) // split vertically?
            if (bit == 0) rect.leftChild   else rect.rightChild // west part or east part
          else
            if (bit == 0) rect.bottomChild else rect.topChild   // south part or north part

        if (!intersects(halfRect)) Nil
        else level(prefix, halfRect, (bitmap << 1) + bit, even ^ 1)
      })
    }

    if (precisions.isEmpty) Set.empty[String]
    else next("") toSet
  }
}
