package usql.dao

import usql.{SqlColumnId, dao}
import usql.profiles.BasicProfile.given
import usql.util.TestBase

class SqlColumnarTest extends TestBase {
  case class Sample(
      name: String,
      age: Int
  )

  "Tabular" should "be derivable" in {
    val tabular = SqlTabular.derived[Sample]
    tabular.columns.map(_.id) shouldBe Seq(SqlColumnId.fromString("name"), SqlColumnId.fromString("age"))
    tabular.tableName shouldBe SqlColumnId.fromString("sample")
  }

  @TableName("samplename")
  case class SampleWithAnnotations(
      @ColumnName("my_name") name: String,
      age: Int
  )

  it should "work with annotations" in {
    val tabular = SqlTabular.derived[SampleWithAnnotations]
    tabular.tableName shouldBe SqlColumnId.fromString("samplename")
    tabular.columns.map(_.id) shouldBe Seq(SqlColumnId.fromString("my_name"), SqlColumnId.fromString("age"))
  }
}
