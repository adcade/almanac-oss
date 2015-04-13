package almanac

import almanac.model._

object TestModels extends App {
  import Criteria._
  val hello = ((fact("f4") is "x") or (fact("f5") is "b")) and ((fact("f6") is "x") or (fact("f5") is "b")) and (fact("f7") is "x")
  println(hello)

  import MetricsQuery._
  import TimeSpan._

  val q: MetricsQuery = select("std.impression")
    .where( (fact("device") is "iphone")
    and (fact("adId") in ("10001", "10002"))
    and (fact("os") like """*ios*""") )
    .groupBy("adId", "site")
    .orderBy("adId", Order.ASC)
    .time(DAY, 1420020000000L, 1420106400000L)
    .limit(1000)
    .skip(2000)
    .query

  println(q)
  import Metric._
  val builderWithFacts = withFacts("adId"->"123", "plId"->"bcd")
  val m = builderWithFacts increment "some.counter"
  val m1 = metric count ("some.counter", 10)

  val m2 = Metric(m.key | TimeSpan.MONTH, m.value + m1.value)

  println(Seq(m, m1, m2))
}
