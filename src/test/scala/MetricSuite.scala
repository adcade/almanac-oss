import almanac.model.TimeSpan
import almanac.model.TimeSpan._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.scalatest.{FunSuite, Matchers}

class MetricSuite extends FunSuite with Matchers {

  test("time span encode and decode") {
    val code = TimeSpan.values.indexOf(DAY)
    code should be (5)
    val span = TimeSpan.values(code)
    span should be (DAY)
  }

  test("time span strip") {
    val raw = new DateTime(2015, 6, 28, 16, 39, 42).withZone(UTC)
    RAW(raw.getMillis) should be(raw.getMillis)

    val minute = raw withSecondOfMinute 0
    MINUTE(raw.getMillis) should be (minute.getMillis)

    val hour = minute withMinuteOfHour 0
    HOUR(raw.getMillis) should be (hour.getMillis)

    val day = hour withHourOfDay 0
    DAY(raw.getMillis) should be (day.getMillis)

    val month = day withDayOfMonth 1
    MONTH(raw.getMillis) should be (month.getMillis)

    val year = month withMonthOfYear 1
    YEAR(raw.getMillis) should be (year.getMillis)
  }
}