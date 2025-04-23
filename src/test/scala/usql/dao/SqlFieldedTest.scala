package usql.dao

import usql.SqlIdentifier
import usql.util.TestBase
import usql.profiles.BasicProfile.*

class SqlFieldedTest extends TestBase {
  case class Coordinate(
      x: Int,
      y: Int
  ) derives SqlFielded

  @TableName("test_person")
  case class Person(
      id: Int,
      @ColumnName("long_name")
      name: String,
      age: Option[Int],
      @ColumnGroup
      coordinate: Coordinate
  ) derives SqlTabular

  object Person extends KeyedCrudBase[Int, Person] {
    override def key: KeyColumnPath = cols.id

    override lazy val tabular: SqlTabular[Person] = summon
  }

  it should "work" in {
    val adapter = summon[SqlFielded[Person]]
    adapter.fields.map(_.fieldName) shouldBe Seq("id", "name", "age", "coordinate")
    adapter.columns
      .map(_.id) shouldBe Seq("id", "long_name", "age", "coordinate_x", "coordinate_y").map(SqlIdentifier.fromString)

    intercept[IllegalStateException] {
      adapter.cols.buildIdentifier
    }
    adapter.cols.name.buildIdentifier shouldBe SqlIdentifier.fromString("long_name")
    Person.cols.name.buildIdentifier shouldBe SqlIdentifier.fromString("long_name")

    adapter.cols.coordinate.x.buildIdentifier shouldBe SqlIdentifier.fromString("coordinate_x")
    Person.cols.coordinate.x.buildIdentifier shouldBe SqlIdentifier.fromString("coordinate_x")
  }
}
