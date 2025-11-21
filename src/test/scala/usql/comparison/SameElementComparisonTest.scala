package usql.comparison

import usql.dao.SqlFielded
import usql.profiles.BasicProfile.*

class SameElementComparisonSpec extends ComparisonTestBase {
  it should "work" in {
    SameElementComparison.compare(Nil, Nil).isSuccess shouldBe true
    SameElementComparison.compare(List(1, 2, 3), List(3, 2, 1)).isSuccess shouldBe true

    val a = SameElementComparison.compare(List(1, 2, 3), List(2, 3, 4))
    a.isFailure shouldBe true
    a.isSuccess shouldBe false

    a.missing shouldBe List(4)
    a.unexpected shouldBe List(1)
    a.matching shouldBe List(2, 3)
  }

  it should "work with duplicates" in {
    val correct = SameElementComparison.compare(List(1, 2, 2, 2, 3, 3), List(2, 1, 2, 3, 2, 3))
    correct.isSuccess shouldBe true

    val incorrect = SameElementComparison.compare(List(2, 1, 2, 3, 4), List(2, 1, 2, 3, 2, 2, 3))
    incorrect.isFailure shouldBe true
    incorrect.matching should contain theSameElementsAs List(1, 2, 2, 3)
    incorrect.missing should contain theSameElementsAs List(2, 2, 3)
    incorrect.unexpected shouldBe List(4)

    given tr: ToTable[Int] = new ToTable[Int] {
      override def columns: Seq[String] = Seq("x")

      override def rows(values: Seq[Int]): Seq[Seq[String]] =
        values.map(v => Seq(v.toString))
    }

    incorrect.renderFailure() shouldBe Some("""Matching: 4 Elements
                                              |Unexpected: 4
                                              |Missing:    2,2,3
                                              |""".stripMargin)
  }

  case class Sample(
      i: Int,
      s: Option[String]
  ) derives SqlFielded

  it should "work with structures" in {
    val correct = SameElementComparison.compare[Sample](Nil, Nil)
    correct.isSuccess shouldBe true

    val sample1 = Seq(
      Sample(1, Some("Hello")),
      Sample(2, None)
    )

    val sample2 = Seq(
      Sample(1, Some("World")),
      Sample(2, None),
      Sample(3, Some("Bob"))
    )

    val sample3 = Seq(
      Sample(2, None),
      Sample(1, Some("Wohoo"))
    )
    SameElementComparison.compare(sample1, sample1.reverse).isSuccess shouldBe true

    val firstDifference = SameElementComparison.compare(sample1, sample2)
    firstDifference.isFailure shouldBe true
    lineTrim(firstDifference.renderFailure().get) shouldBe
      lineTrim(
        """|Matching: 1 Elements
           |Unexpected:
           ||i|s    |
           ||-|-----|
           ||1|Hello|
           |
           |Missing:
           ||i|s    |
           ||-|-----|
           ||1|World|
           ||3|Bob  |
           |
           |""".stripMargin
      )

    val singleDifference = SameElementComparison.compare(sample1, sample3)
    singleDifference.isFailure shouldBe true
    lineTrim(singleDifference.renderFailure().get) shouldBe
      lineTrim("""Matching: 1 Elements
                 |Unexpected:
                 ||i|s    |
                 ||-|-----|
                 ||1|Hello|
                 |
                 |Missing:
                 ||i|s    |
                 ||-|-----|
                 ||1|Wohoo|
                 |
                 |Comparison:
                 ||Key|Expected|Got  |
                 ||---|--------|-----|
                 ||s  |Wohoo   |Hello|
                 |
                 |""".stripMargin)
  }
}
