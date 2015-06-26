package almanac.model

object GeoHash {
  private val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

  def decodeBounds(geohash: String): ((Double, Double), (Double, Double)) = {
    def toBitList: List[Boolean] = geohash.flatMap {
      c => ("00000" + base32.indexOf(c).toBinaryString ).
        reverse.take(5).reverse.map('1' == ) } toList

    def split(l: List[Boolean]): (List[Boolean], List[Boolean]) = l match {
      case Nil => (Nil,Nil)
      case x::Nil => ( x::Nil,Nil)
      case x::y::zs => {
        val (xs,ys) = split( zs )
        (x::xs,y::ys)
      }
    }

    def dehash(xs: List[Boolean], min: Double, max: Double): (Double, Double) = {
      ((min, max) /: xs ) {
        case ((min, max), b) =>
          if (b) ( (min+max)/2, max )
          else ( min, (min+max)/2 )
      }
    }

    val (xs, ys) = split(toBitList)
    ( dehash(ys, -90, 90), dehash(xs, -180, 180) )
  }

  def decode(geohash: String): (Double,Double) = {
    decodeBounds(geohash) match {
      case ((minLat,maxLat),(minLng,maxLng)) => ( (maxLat+minLat)/2, (maxLng+minLng)/2 )
    }
  }

  def encode(lat: Double, lng: Double, precision: Int = 12) = {
    var (minLat,maxLat) = (-90.0,90.0)
    var (minLng,maxLng) = (-180.0,180.0)
    val bits = List(16,8,4,2,1)

    (0 until precision).map{ p => {
      base32 apply (0 until 5).map{ i => {
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

  def round(geohash: String, precision: Int) = geohash.substring(0, precision)
}

object Coordinate {
  def apply(geohash: String) = {
    val (lat, lng) = GeoHash.decode(geohash)
    new Coordinate(lat, lng)
  }
  def isValidLat(lat: Double): Boolean = lat <= 90 && lat >= -90
  def isValidLng(lng: Double): Boolean = lng <= 180 && lng >= -180

  /**
   * whether the range from west to east across IDL (International Date Line)
   * @param westLng left bound longitude of the range
   * @param eastLng right bound longitude of the range
   * @return a boolean indicates whether this range goes thru international date line
   */
  def acrossIDL(westLng: Double, eastLng: Double) = westLng - eastLng > 180
}

case class Coordinate(lat: Double, lng: Double) {
  import Coordinate._
  if (!isValidLat(lat)) throw new IllegalArgumentException("latitude out of bound [-90, 90]")
  if (!isValidLng(lng)) throw new IllegalArgumentException("longitude out of bound [-180, 180]")

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

  lazy val geohash = GeoHash.encode(lat, lng)
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
  if (!isValidLat(north) || !isValidLat(south)) throw new IllegalArgumentException("latitude out of bound [-90, 90]")
  if (!isValidLng(east) || !isValidLng(west)) throw new IllegalArgumentException("longitude out of bound [-180, 180]")
  if (north < south) throw new IllegalArgumentException("latitude north < south.")

  def top    = north
  def bottom = south
  def left   = west
  def right  = east

  def encompass(co: Coordinate) = (co eastOf west) && (co westOf east) && (co southOf north) && (co northOf south)
}

sealed abstract class GeoPrecision(private val digit: Int) extends Serializable {
  def apply(optionalPos: Option[Coordinate]) = optionalPos match {
    case Some(pos) => if (digit == -1) Some(Coordinate(pos.lat, pos.lng))
    else if (digit == 0) None
    else Some(Coordinate(round(pos.lat), round(pos.lng)))
    case None => None
  }

  private def round(value: Double) = math.floor(value * digit) / digit
}

object GeoPrecision {
  case object Unrounded  extends GeoPrecision(-1)
  case object Thousandth extends GeoPrecision(1000)
  case object Hundredth  extends GeoPrecision(100)
  case object Tenth      extends GeoPrecision(10)
  case object Degree     extends GeoPrecision(1)
  case object Wordwide   extends GeoPrecision(0)

  lazy val values = Seq(Unrounded, Thousandth, Hundredth, Tenth, Degree, Wordwide)
  private lazy val lookup = Map(values map {s => (s.digit, s)}: _*)
  def toPrecision(digit: Int) = lookup(digit)
}