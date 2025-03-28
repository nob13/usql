package usql.dao

import usql.SqlIdentifier
import usql.util.TestBase
import usql.profiles.BasicProfile.*

class SqlFieldedTest extends TestBase {
  case class Person(
      id: Int,
      @ColumnName("long_name")
      name: String,
      age: Option[Int]
  ) derives SqlFielded

  it should "work" in {
    val adapter = summon[SqlFielded[Person]]
    summon[SqlFielded[Person]].fields.map(_.fieldName) shouldBe Seq("id", "name", "age")

    intercept[IllegalStateException] {
      adapter.cols.id
    }
    adapter.cols.name.id shouldBe SqlIdentifier.fromString("long_name")
  }
}
