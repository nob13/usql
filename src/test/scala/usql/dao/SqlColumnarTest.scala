package usql.dao

import usql.{SqlIdentifier, dao}
import usql.profiles.BasicProfile.given
import usql.util.TestBase

class SqlColumnarTest extends TestBase {
  case class Sample(
      name: String,
      age: Int
  )

  "Tabular" should "be derivable" in {
    val tabular = SqlTabular.derived[Sample]
    tabular.columns.map(_.id) shouldBe Seq(SqlIdentifier.fromString("name"), SqlIdentifier.fromString("age"))
    tabular.tableName shouldBe SqlIdentifier.fromString("sample")
  }

  @TableName("samplename")
  case class SampleWithAnnotations(
      @ColumnName("my_name") name: String,
      age: Int
  )

  it should "work with annotations" in {
    val tabular = SqlTabular.derived[SampleWithAnnotations]
    tabular.tableName shouldBe SqlIdentifier.fromString("samplename")
    tabular.columns.map(_.id) shouldBe Seq(SqlIdentifier.fromString("my_name"), SqlIdentifier.fromString("age"))
  }
}
