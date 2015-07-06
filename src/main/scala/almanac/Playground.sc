import almanac.model.{Coordinate, GeoRect}
object Playground {
  1::2::Nil

  val BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

//  fact("f1") in    ("a", "a", "b")
//  fact("f1") notIn ("a", "a", "b")
//  fact("f1") is    "a"
//  fact("f1") isNot "a"
//  fact("f1") like  """^\d*"""
//  (fact("f1") isNot "a") and (fact("f2") notIn("a", "b", "c"))
//  .and ((fact("f3") is "b") and (fact("f3") is "c"))
//    all(fact("f3").is("c")).and(fact("f5").is("c"))
//  (fact("f1") is "a").and(fact("f1") is "c") and (fact("f1") is "d") and (fact("f1") is "d")
//    .and(all(fact("f2") is "x", fact("f3") is "x") and ((fact("f4") is "x") or (fact("f5") is "b")))
//  val hello = ((fact("f4") is "x") or (fact("f5") is "b")) and ((fact("f4") is "x") or (fact("f5") is "b"))
//  hello.criteria
//  def round(num: Double)(digit: Int) = {
//    math.floor(num * digit) / digit
//  }
//  round(10.9329)(100)
//  GeoRect(40.760650634765625, -73.98056030273438, 40.752410888671875, -73.99429321289062)
//    .geohashes(6)
  val target = GeoRect("g")
  def toBitString(b: Int): String = if (b == 0) "" else toBitString(b >> 1) + (b & 1)
  def level(prefix:String, rect: GeoRect, bitmap: Int, even: Int): Seq[String] = {
    println(prefix, rect, toBitString(bitmap), even)
    if (bitmap >= 32) {
      println(prefix + BASE32(bitmap - 32), rect, toBitString(bitmap), even)
      Seq(prefix + BASE32(bitmap - 32))
    }
    else Seq(0, 1) flatMap (bit => {
      val halfRect =
        if ((prefix.length * 5 + even) % 2 == 0) // split vertically?
          if (bit == 0) rect.leftChild else rect.rightChild // west part or east part
        else
          if (bit == 0) rect.bottomChild else rect.topChild // south part or north part

      if (!(target intersects halfRect)) Nil
      else level(prefix, halfRect, (bitmap << 1) + bit, even ^ 1)
    })
  }
  target
  level("", GeoRect(""), 1, 0)
  val rect = GeoRect(40.760650634765625,
                     -73.98056030273438,
                     40.752410888671875,
                     -73.99429321289062)
  Coordinate(rect.latMiddle, rect.lngMiddle)


  val set_7_6 = Set(
    "dr5ruhp", "dr5ruk0", "dr5ruk1", "dr5ruk4", "dr5ruk5", "dr5rukh", "dr5rukj", "dr5rukn", "dr5rukp", "dr5rus0",
    "dr5ru5z",                                                                                         "dr5rueb",
    "dr5ru5x",                                         "dr5ru7",                                       "dr5rue8",
    "dr5ru5r",                                                                                         "dr5rue2",
    "dr5ru5p",                                                                                         "dr5rue0",
    "dr5ru4z", "dr5ru6b", "dr5ru6c", "dr5ru6f", "dr5ru6g", "dr5ru6u", "dr5ru6v", "dr5ru6y", "dr5ru6z", "dr5rudb")

  val order = new Ordering[String] {
    override def compare(x: String, y: String): Int = {
      if (x.length == y.length) x compare y
      else x.length - y.length
    }
  }

  set_7_6.toList.sorted(order)
  rect.geohashes(Set(6, 7)).toList.sorted(order)
}