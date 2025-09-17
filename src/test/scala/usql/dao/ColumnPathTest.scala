package usql.dao

import usql.SqlIdentifier
import usql.util.TestBase
import usql.profiles.BasicProfile.*

class ColumnPathTest extends TestBase {

  case class SubSubElement(
      foo: Boolean
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
        true
      )
    )
  )

  it should "fetch identifiers" in {
    path.x.buildIdentifier shouldBe SqlIdentifier.fromString("x")
    path.sub.a.buildIdentifier shouldBe SqlIdentifier.fromString("a")
    intercept[IllegalStateException] {
      path.sub.sub2.buildIdentifier
    }
    path.sub.sub2.foo.buildIdentifier shouldBe SqlIdentifier.fromString("sub2_foo")
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
    // TODO
  }
}
