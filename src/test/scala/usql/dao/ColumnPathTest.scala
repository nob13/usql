package usql.dao

import usql.SqlIdentifier
import usql.util.TestBase
import usql.profiles.BasicProfile.*

class ColumnPathTest extends TestBase {

  case class SubSubElement(
      foo: Boolean,
      bar: Int
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
        123
      )
    )
  )

  it should "fetch identifiers" in {
    path.x.buildIdentifier shouldBe Seq(SqlIdentifier.fromString("x"))
    path.sub.a.buildIdentifier shouldBe Seq(SqlIdentifier.fromString("a"))
    path.sub.sub2.buildIdentifier shouldBe Seq(
      SqlIdentifier.fromString("sub2_foo"),
      SqlIdentifier.fromString("sub2_bar")
    )
    path.sub.sub2.foo.buildIdentifier shouldBe Seq(SqlIdentifier.fromString("sub2_foo"))
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
    // TODO
  }

  it should "provide a fielded for each" in {
    // Fielded of single projection --> Column
    // Fielded of empty tuple --> Empty
    // Fielded of one tuple --> The one of the parent
    // Sub fields --> Fielded, if possible
    // Not columnar, as we have logical names for groups
    
    // Aber es kann auch ein Ein-Tupel sein!
    
    // TODO
  }
}
