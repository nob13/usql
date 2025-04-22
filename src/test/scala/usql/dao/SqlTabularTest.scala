package usql.dao

import usql.*
import usql.util.TestBase
import usql.profiles.BasicProfile.*

class SqlTabularTest extends TestBase {
  case class Nested(
      x: Double,
      y: Double
  ) derives SqlFielded

  case class WithNested(
      @ColumnGroup(ColumnGroupMapping.Pattern(pattern = "a_%c"))
      a: Nested,
      @ColumnGroup(ColumnGroupMapping.Pattern(pattern = "%c_s"))
      b: Nested,
      c: Nested
  )

  it should "work for nested" in {
    val tabular = SqlTabular.derived[WithNested]
    tabular.rowEncoder.cardinality shouldBe 6
    tabular.rowDecoder.cardinality shouldBe 6
    tabular.columns.map(_.id) shouldBe Seq("a_x", "a_y", "x_s", "y_s", "c_x", "c_y").map(SqlIdentifier.fromString)
  }
}
