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
    summon[ColumnPath.BuildFromTuple[Sample, EmptyTuple]] // geht
    summon[ColumnPath.BuildFromTuple[Sample, Tuple1[ColumnPath[Sample, Int]]]] // geht
    val s2                                 = summon[ColumnPath.BuildFromTuple[Sample, Tuple2[ColumnPath[Sample, Int], ColumnPath[Sample, Int]]]] // geht
    val r2: ColumnPath[Sample, (Int, Int)] = s2.build(???)
    /*
    val foo1: ColumnPath[Sample, EmptyTuple] = EmptyTuple
    val foo2: ColumnPath[Sample, (Int)] = (path.x)
    summon[ColumnPath.BuildFromTuple[Sample, EmptyTuple]]
    val s3 = ColumnPath.buildFromIteration[Sample, Int, EmptyTuple]
    // summon[ColumnPath.BuildFromTuple[Sample, Tuple1[Int]]]// crashes
    val blub: ColumnPath.BuildFromTuple[Sample, Tuple1[Int]] = ColumnPath.buildFromIteration[Sample, Int, EmptyTuple]
    val s4 = ColumnPath.buildFromIteration[Sample, Int, Tuple1[Int]]
    summon[ColumnPath.BuildFromTuple[Sample, Tuple1[Int]]]
    val foo3: ColumnPath[Sample, (Int, Int)] = (path.x, path.y)
     */
    val blubbi = summon[ColumnPath.BuildFromTuple[Sample, Tuple2[ColumnPath[Sample, Int], ColumnPath[Sample, Int]]]]
    val foo: Tuple2[ColumnPath[Sample, Int], ColumnPath[Sample, Int]] = (path.x, path.y)

    
    val result: ColumnPath[Sample, (Int, Int)] = ColumnPath.fromTuple[
      Sample, 
      Tuple2[ColumnPath[Sample, Int], ColumnPath[Sample, Int]]
    ](foo)

    val result2: ColumnPath[Sample, (Int, Int)] = ColumnPath.fromTuple(foo)
    
    val resultX: ColumnPath[Sample, (Int, Int)] = ColumnPath.fromTuple(foo)(using blubbi) // TODO

  }

  it should "work with optionals" in {
    // TODO
  }

  it should "provide a fielded for each" in {
    // TODO
  }
}
