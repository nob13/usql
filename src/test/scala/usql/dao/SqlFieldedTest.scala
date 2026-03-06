package usql.dao

import usql.SqlColumnId
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
      coordinate: Coordinate,
      @ColumnGroup
      ocoordinate: Option[Coordinate] = None
  ) derives SqlTabular

  object Person extends KeyedCrudBase[Int, Person] {
    override def key: KeyColumnPath = cols.id

    override lazy val tabular: SqlTabular[Person] = summon
  }

  it should "work" in {
    val adapter = summon[SqlFielded[Person]]
    adapter.fields.map(_.fieldName) shouldBe Seq("id", "name", "age", "coordinate", "ocoordinate")
    adapter.columns
      .map(_.id) shouldBe SqlColumnId.fromStrings(
      "id",
      "long_name",
      "age",
      "coordinate_x",
      "coordinate_y",
      "ocoordinate_x",
      "ocoordinate_y"
    )

    adapter.cols.columnIds shouldBe SqlColumnId.fromStrings(
      "id",
      "long_name",
      "age",
      "coordinate_x",
      "coordinate_y",
      "ocoordinate_x",
      "ocoordinate_y"
    )
    adapter.cols.name.columnIds shouldBe SqlColumnId.fromStrings("long_name")
    Person.cols.name.columnIds shouldBe SqlColumnId.fromStrings("long_name")

    adapter.cols.coordinate.x.columnIds shouldBe SqlColumnId.fromStrings("coordinate_x")
    Person.cols.coordinate.x.columnIds shouldBe SqlColumnId.fromStrings("coordinate_x")

    adapter.cols.ocoordinate.x.columnIds shouldBe SqlColumnId.fromStrings("ocoordinate_x")
    Person.cols.ocoordinate.x.columnIds shouldBe SqlColumnId.fromStrings("ocoordinate_x")
  }
}
