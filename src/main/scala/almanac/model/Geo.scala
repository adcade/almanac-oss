package almanac.model

import almanac.model.GeoHash._

object GeoHash {
  type Bounds = (Double, Double)

  implicit class BoundsExtension(b1: Bounds) {
    def lower = b1._1
    def upper = b1._2

    /**
     *
     * @param b2
     * @return
     */
    def latIntersects(b2: Bounds): Boolean =
      (b1.lower <= b2.lower && b2.lower <  b1.upper) ||
      (b1.lower <  b2.upper && b2.upper <= b1.upper) ||
      (b2.lower <= b1.lower && b1.lower <  b2.upper) ||
      (b2.lower <  b1.upper && b1.upper <= b2.upper)


    /**
     * This is different from latitude intersects mainly because of the cyclic natural of longitude
     *
     * if IDL is in between [l1, r1] (l1 r1 can't equals to IDL in the same time)
     *   if l2/r2 E (l1, IDL] U [IDL, r1] then they intersects
     * if IDL is in between [l2, r2]
     *   if l1/r1 E (l2, IDL] U [IDL, r2] then they intersects
     *
     * @param b2
     * @return
     */
    def lngIntersects(b2: Bounds): Boolean =
      (b1.lower <= b2.lower && b2.lower <  b1.upper) || // l2 E (l1, r1]
      (b1.lower <  b2.upper && b2.upper <= b1.upper) || // r2 E [l1, r1)
      (b2.lower <= b1.lower && b1.lower <  b2.upper) || // l1 E (l2, r2]
      (b2.lower <  b1.upper && b1.upper <= b2.upper) || // r1 E [l2, r2)
      (b1.lower > b1.upper && !(b1.lower == 180 && b1.upper == -180)) && ( // if IDL E [l1, r1]
        b1.lower <= b2.lower || b2.lower <  b1.upper || // l2 E (l1, IDL] U [IDL, r1]
        b1.lower <  b2.upper || b2.upper <= b1.upper    // r2 E [l1, IDL] U [IDL, r1)
      ) ||
      (b2.lower > b2.upper && !(b2.lower == 180 && b2.upper == -180)) && ( // if IDL E [l1, r1]
        b2.lower <= b1.lower || b1.lower <  b2.upper || // l2 E (l1, IDL] U [IDL, r1]
        b2.lower <  b1.upper || b1.upper <= b2.upper    // r2 E [l1, IDL] U [IDL, r1)
      )

    /**
     *
     * @param b2
     * @return
     */
    def latContains(b2: Bounds): Boolean =
      b1.lower <= b2.lower && b2.lower < b1.upper && b1.lower < b2.upper && b2.upper <= b1.upper // b2 E [b1, t1) && t2 E (b1, t1]

    /**
     *
     * @param b2
     * @return
     */
    def lngContains(b2: Bounds): Boolean =
      if (b1.lower <= b1.upper) b1.lower <= b2.lower && b2.lower <= b2.upper && b2.upper <= b1.upper // [l1 -> l2 -> r2 -> r1]
      else (b1.lower <= b2.lower && b2.lower <= b2.upper) || // [l1 -> l2 -> r2 -> IDL -> r1]
           (b1.lower <= b2.lower && b2.upper <= b1.upper) || // [l1 -> l2 -> IDL -> r2 -> r1]
           (b2.lower <= b2.upper && b2.upper <= b1.upper)    // [l1 -> IDL -> l2 -> r2 -> r1]
  }

  val LAT_RANGE = (-90.0, 90.0)
  val LNG_RANGE = (-180.0, 180.0)

  def nullGeohash(precision: Int) = "------------".substring(0, precision)

  val GLOBAL = 0
  val MAX_PRECISION = 12

  lazy val ALL_PRECISION = (GLOBAL to MAX_PRECISION).toSet

  implicit class CoordinateWrapper(x: Double) {
    def in(b: Bounds): Boolean = x >= b._1 && x <= b._2
    def westOf(otherLng: Double): Boolean = if (x < otherLng) x + 180 > otherLng
                                            else if (x > 0 && otherLng <0) otherLng + 180 < x
                                            else false
    def eastOf(otherLng: Double): Boolean = !westOf(otherLng) && x!=otherLng
    def northOf(otherLat: Double): Boolean = otherLat < x
    def southOf(otherLat: Double): Boolean = !northOf(otherLat) && x!=otherLat
  }

  val BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

  /**
   *
   * @param geohash
   * @return
   */
  def toBounds(geohash: String): (Bounds, Bounds) = {
    require(geohash.nonNullGeohash, "null geohash has no location")

    def charToInt(ch: Char) = BASE32.indexOf(ch)
    def intToBits(i: Int) = 4 to 0 by -1 map (i >> _ & 1) map (_==1)
    def deinterleave[T](l: List[T]) = (l :\ (List[T](), List[T]())) { case (b, (list1, list2)) => (b :: list2, list1) }
    def fromBits(bits: List[Boolean], bounds: Bounds): Bounds = (bounds /: bits ) {
      case ((min, max), bool) =>
        if (bool) ((min+max)/2, max)
        else      (min, (min+max)/2)
    }

    val (lngBits, latBits) = deinterleave((geohash map charToInt flatMap intToBits).toList)
    (fromBits(latBits, LAT_RANGE), fromBits(lngBits, LNG_RANGE))
  }

  /**
   *
   * @param geohash
   * @return
   */
  def decode(geohash: String): (Double, Double) = {
    toBounds(geohash) match {
      case ((minLat, maxLat), (minLng, maxLng)) => ( (maxLat+minLat)/2, (maxLng+minLng)/2 )
    }
  }

  /**
   *
   * @param lat
   * @param lng
   * @param precision
   * @return
   */
  def encode(lat: Double, lng: Double, precision: Int = MAX_PRECISION) = {
    require(lat in LAT_RANGE, "Latitude out of range")
    require(lng in LNG_RANGE, "Longitude out of range")
    require(precision >= 0, "Precision must be a positive integer")

    def interleave[T](list1: List[T], list2: List[T]): List[T] = (list1, list2) match {
      case (x, Nil) => x
      case (Nil, y) => y
      case (x :: xs, y :: ys) => x :: y :: interleave(xs, ys)
      case _ => Nil
    }

    def toBits(magnitude: Double, bounds: Bounds, numBits: Int): List[Int] = {
      val (min, max) = bounds
      if (numBits == 0) Nil
      else {
        val mid = (min + max) / 2
        if (magnitude >= mid) 1 :: toBits(magnitude, (mid, max), numBits - 1)
        else                  0 :: toBits(magnitude, (min, mid), numBits - 1)
      }
    }

    val numBits = precision * 5 / 2
    val latBits = toBits(lat, LAT_RANGE, numBits)
    val lngBits = toBits(lng, LNG_RANGE, numBits + precision % 2)
    (interleave(lngBits, latBits) grouped(5) map { x =>
      BASE32(x reduceLeft { (acc, b) => (acc << 1) + b})
    }).mkString
  }

  implicit class GeoHashHelper(geohash: String) {

    def isNullGeohash: Boolean = geohash.startsWith(nullGeohash(1))
    def nonNullGeohash: Boolean = !isNullGeohash

    /**
     *
     * @param precision
     * @return
     */
    def roundOrPad(precision: Int): String = {
      require(precision >= 0)
      if (precision == 0) "" // TODO: even if no geohash provided, it has to happened somewhere in this world!?
      else {
        val targetPrecision = math.min(precision, MAX_PRECISION)
        val delta = targetPrecision - geohash.length
        val sb = new StringBuilder(geohash)

        if (delta <= 0) sb.substring(0, targetPrecision)
        else if (geohash.isNullGeohash) nullGeohash(targetPrecision)
        else ((sb append "s") /: (1 until delta)) { (sb, _) => sb append "0" }.toString
      }
    }

    /**
     * find the near sub geohashes, could be the geohash itself if precision level exist
     *
     * @param precisions a set of precisions the geohashes should ranged in
     * @return
     */
    def subGeohashes(precisions: Set[Int] = ALL_PRECISION): Seq[String] =
      if (geohash.isNullGeohash) Nil // null geohash has no sub-geohashes
      else if (precisions contains geohash.length) Seq(geohash)
      else if (geohash.length + 1 > precisions.max) Nil // no deeper search
      else BASE32 flatMap { char => (geohash + char).subGeohashes(precisions) }

    /**
     * find the near super geohash, could be the geohash itself if precision level exist
     *
     * if none of the upper layer precision is present, return None
     *
     * @param precisions a set of precisions the geohashes should ranged in
     * @return Optional superGeohash result
     */
    def superGeohash(precisions: Set[Int] = ALL_PRECISION): Option[String] =
      if (geohash.isNullGeohash) None // null geohash has no super-geohashes
      else if (precisions contains geohash.length) Some(geohash)
      else if (geohash.isEmpty) None // you can't have super geohash of empty String
      else geohash.substring(0, geohash.length - 1).superGeohash(precisions)

    /**
     * convenient method for `roundOrPad`
     *
     * @param precision
     * @return
     */
    def ~(precision: Int) = roundOrPad(precision)
  }
}

object Coordinate {
  implicit class CoordinateBuilder(latitude: Double) {

    /**
     * latitude intersect longitude, to build a coordinate
     * let's say you have:
     * {{{
     *  val lat = 3.0;
     *  val lng = 4.0
     * }}}
     * then
     * {{{ lat x lng }}} will return you a `Coordinate`
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

trait GeoShape {
  def intersects(that: GeoRect): Boolean
  def intersects(geohash: String): Boolean
  def contains(co: Coordinate): Boolean
  def contains(that: GeoRect): Boolean
  def contains(geohash: String): Boolean

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
      if (gh.length >= precisions.max) Seq(gh.superGeohash(precisions)).flatten
      else if (this contains gh) gh.subGeohashes(precisions)
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
    else next("").toSet
  }
}

object GeoRect {
  def apply(geohash: String): GeoRect = {
    require(geohash.nonNullGeohash, "null geohash has no coresponding rectangle")
    val ((minLat, maxLat), (minLng, maxLng)) = toBounds(geohash)
    GeoRect(maxLat, maxLng, minLat, minLng)
  }

  def apply(latBounds: Bounds, lngBounds: Bounds): GeoRect = {
    val ((minLat, maxLat), (minLng, maxLng)) = (latBounds, lngBounds)
    GeoRect(maxLat, maxLng, minLat, minLng)
  }

  /**
   * giving arbitary two coordinate and get the rectangle they form.
   * the coordinate given could be any of the following:
   *  top-left, top-right, bottom-left, bottom-right
   *
   * warning this class is convenient, but here is the caveats:
   *
   * It is ambious situation where the bigger rectangle is desired or the smaller one!
   * for example, give `Coordinate(45, 20)` and `Coordinate(30, -20)` there is two possible rectangle:
   *
   *  `GeoRect(45, 20, 30, -20)` and `GeoRect(45, -20, 30, -20)`
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

case class GeoRect(top: Double, right: Double, bottom: Double, left: Double) extends GeoShape {
  require((north in LAT_RANGE) && (south in LAT_RANGE), "latitude out of bound [-90, 90]")
  require((east in LNG_RANGE) && (west in LNG_RANGE), "longitude out of bound [-180, 180]")
  require(north >= south, "latitude north < south.")

  // region properties

  def north = top
  def south = bottom
  def west  = left
  def east  = right

  def latMiddle = (north + south) / 2
  def lngMiddle = {
    val middle = (east + west) / 2
    if (east >= west) middle
    else if (middle > 0) middle - 180
    else middle + 180
  }

  def topChild    = GeoRect(top, right, latMiddle, left)
  def bottomChild = GeoRect(latMiddle, right, bottom, left)
  def leftChild   = GeoRect(top, lngMiddle, bottom, left)
  def rightChild  = GeoRect(top, right, bottom, lngMiddle)

  def latBounds = (bottom, top)
  def lngBounds = (left, right)

  // endregion

  /**
   *
   * @param that
   * @return
   */
  def intersects(that: GeoRect): Boolean =
    latBounds.latIntersects(that.latBounds) &&
    lngBounds.lngIntersects(that.lngBounds)

  /**
   *
   * @param geohash
   * @return
   */
  def intersects(geohash: String): Boolean = intersects(GeoRect(geohash))

  /**
   *
   * @param co
   * @return
   */
  def contains(co: Coordinate): Boolean = ((co.lng eastOf west) || (co.lng == west)) &&
                                          ((co.lng westOf east) || (co.lng == east)) &&
                                          ((co.lat southOf north) || (co.lat == north)) &&
                                          ((co.lat northOf south) || (co.lat == south))

  /**
   *
   * @param that
   * @return
   */
  def contains(that: GeoRect): Boolean =
    (latBounds latContains that.latBounds) &&
    (lngBounds lngContains that.lngBounds)

  /**
   *
   * @param geohash
   * @return
   */
  def contains(geohash: String): Boolean = contains(GeoRect(geohash))

  val lnOf2 = math.log(2)

  /**
   * log2 of a product of numbers, based on a series of theorems:
   *
   * log2(x) = log(x) / log(2)
   *
   * log2(a * b) = log2(a) + log2(b)
   *
   * @param num
   * @return
   */
  private def log2(num: Double*) = (num map math.log).sum / lnOf2

  private def precision = log2(360 / (right-left), 180 / (top-bottom)) / 5

  // TODO:
  def defaultGeohashes = geohashes(Set(math.ceil(precision).toInt + 1))
}
