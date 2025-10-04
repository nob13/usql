package usql.dao

import usql.{DataType, SqlColumnId}
import usql.util.TestBase
import usql.profiles.BasicProfile.*

class ColumnPathTest extends TestBase {

  case class SubSubElement(
      foo: Boolean,
      bar: Int,
      biz: Option[String]
  ) derives SqlFielded

  case class SubElement(
      a: Int,
      b: String,
      @ColumnGroup
      sub2: SubSubElement
  ) derives SqlFielded

  case class Sample(
      x: Int,
      y: Int,
      @ColumnGroup(ColumnGroupMapping.Anonymous)
      sub: SubElement
  ) derives SqlFielded

  val path: ColumnPath[Sample, Sample] = ColumnPath.make

  val sample = Sample(
    100,
    200,
    sub = SubElement(
      a = 101,
      b = "Hello",
      sub2 = SubSubElement(
        true,
        123,
        Some("Hallo")
      )
    )
  )

  it should "fetch identifiers" in {
    path.x.columnIds shouldBe Seq(SqlColumnId.fromString("x"))
    path.sub.a.columnIds shouldBe Seq(SqlColumnId.fromString("a"))
    path.sub.sub2.columnIds shouldBe Seq(
      SqlColumnId.fromString("sub2_foo"),
      SqlColumnId.fromString("sub2_bar"),
      SqlColumnId.fromString("sub2_biz")
    )
    path.sub.sub2.foo.columnIds shouldBe Seq(SqlColumnId.fromString("sub2_foo"))
  }

  it should "fetch elements" in {
    val getter1 = path.x.buildGetter
    val getter2 = path.sub.sub2.foo.buildGetter
    getter1(sample) shouldBe 100
    getter2(sample) shouldBe true
  }

  it should "work with tuples" in {
    val empty: ColumnPath[Sample, EmptyTuple] = EmptyTuple
    empty.buildGetter(sample) shouldBe EmptyTuple

    val pair = (path.x, path.y)
    pair.buildGetter(sample) shouldBe (100, 200)
    pair._1.buildGetter(sample) shouldBe 100
    pair._2.buildGetter(sample) shouldBe 200
  }

  it should "work with optionals" in {
    val rootPath: ColumnPath[Option[Sample], Option[Sample]] = ColumnPath.makeOpt

    val x = rootPath.x

    x.structure.columns shouldBe Seq(
      SqlColumn("x", DataType.get[Option[Int]])
    )

    val getter = x.buildGetter
    getter(None) shouldBe None
    getter(Some(sample)) shouldBe Some(sample.x)

    val sub2: ColumnPath[Option[Sample], Option[SubSubElement]] = rootPath.sub.sub2
    val sub2Structure                                           = sub2.structure.asInstanceOf[SqlFielded[Option[SubSubElement]]]

    sub2Structure.columns shouldBe Seq(
      SqlColumn(SqlColumnId.fromString("sub2_foo"), DataType.get[Option[Boolean]]),
      SqlColumn(SqlColumnId.fromString("sub2_bar"), DataType.get[Option[Int]]),
      SqlColumn(SqlColumnId.fromString("sub2_biz"), DataType.get[Option[String]])
    )

    sub2Structure.split(None) shouldBe Seq(None, None, None)
    sub2Structure.build(Seq(None, None, None)) shouldBe None
    sub2Structure.build(Seq(Some(true), Some(100), Some("Bim"))) shouldBe Some(SubSubElement(true, 100, Some("Bim")))
    sub2Structure.build(Seq(Some(true), Some(100), None)) shouldBe Some(SubSubElement(true, 100, None))
    intercept[IllegalArgumentException] {
      // The first value is not optional inside
      sub2Structure.build(Seq(None, Some(100), None))
    }

    sub2.buildGetter(None) shouldBe None
    sub2.buildGetter(Some(sample)) shouldBe Some(sample.sub.sub2)

    // Note: UnpackOption helps here, that the type is a String, not Option[String]
    val biz: ColumnPath[Option[Sample], Option[String]] = sub2.biz
    biz.structure.columns shouldBe Seq(
      SqlColumn(SqlColumnId.fromString("sub2_biz"), DataType.get[Option[String]])
    )

    biz.buildGetter(None) shouldBe None

    biz.buildGetter(Some(sample)) shouldBe Some("Hallo")
    biz.buildGetter(Some(sample.copy(sub = sample.sub.copy(sub2 = sample.sub.sub2.copy(biz = None))))) shouldBe None
  }

  it should "provide a structure for each" in {
    path.x.structure shouldBe SqlColumn("x", DataType.get[Int])
    path.sub.sub2.foo.structure shouldBe SqlColumn("sub2_foo", DataType.get[Boolean])

    val substructure: SqlFielded[SubSubElement] = path.sub.sub2.structure.asInstanceOf[SqlFielded[SubSubElement]]
    substructure.columns.map(_.id.name) shouldBe Seq(
      "sub2_foo",
      "sub2_bar",
      "sub2_biz"
    )
    path.structure shouldBe Sample.derived$SqlFielded
    val subStructure                            = path.sub.structure.asInstanceOf[SqlFielded[SubElement]]

    val pair: ColumnPath[Sample, (Int, Int)] = (path.x, path.y)
    pair.structure.columns.map(_.id.name) shouldBe Seq(
      "x",
      "y"
    )
    pair.structure.asInstanceOf[SqlFielded[(Int, Int)]].fields.map(_.fieldName) shouldBe Seq("_1", "_2")
  }

  "concat" should "work" in {
    val first     = ColumnPath.make[Sample].sub.sub2
    val second    = ColumnPath.make[SubSubElement].foo
    val concatted = ColumnPath.concat(first, second)
    concatted.structure.columns shouldBe Seq(
      SqlColumn(SqlColumnId.fromString("sub2_foo"), DataType.get[Boolean])
    )
  }

  it should "work for optionalized values" in {
    val first     = ColumnPath.makeOpt[Sample].sub.sub2
    val second    = ColumnPath.makeOpt[SubSubElement].foo
    val concatted = ColumnPath.concat(first, second)
    concatted.structure.columns shouldBe Seq(
      SqlColumn(SqlColumnId.fromString("sub2_foo"), DataType.get[Option[Boolean]])
    )
    concatted.buildGetter(None) shouldBe None
    concatted.buildGetter(Some(sample)) shouldBe Some(sample.sub.sub2.foo)
  }
}
