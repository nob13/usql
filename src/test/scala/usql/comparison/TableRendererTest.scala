package usql.comparison

import usql.util.TestBase

class TableRendererTest extends TestBase {

  val sampleTable = Table(
    columns = IndexedSeq("Hello", "World", "Very\nLong"),
    rows = IndexedSeq(
      IndexedSeq(Some("How"), Some("Are"), None),
      IndexedSeq(None, Some("You\n?"))
    )
  )

  "render" should "work per by default" in {
    lineTrim(TableRenderer.default.render(sampleTable)) shouldBe
      lineTrim(
        """||Hello|World |Very\nLong|
           ||-----|------|----------|
           ||How  |Are   |n.A.      |
           ||n.A. |You\n?|n.A.      |
           |""".stripMargin
      )
  }

  it should "work with short max size" in {
    val got = TableRenderer(TableRenderer.Settings(maxCellWidth = 4)).render(sampleTable)
    lineTrim(got) shouldBe
      lineTrim(
        """||H...|W...|V...|
           ||----|----|----|
           ||How |Are |n.A.|
           ||n.A.|Y...|n.A.|
           |""".stripMargin
      )
  }

  it should "not crash on absurd small values" in {
    val got = TableRenderer(TableRenderer.Settings(maxCellWidth = 1)).render(sampleTable)
    lineTrim(got) shouldBe
      lineTrim(
        """||.|.|.|
           ||-|-|-|
           ||.|.|.|
           ||.|.|.|
           |""".stripMargin
      )
  }

  it should "not crash on 0" in {
    val got = TableRenderer(TableRenderer.Settings(maxCellWidth = 0)).render(sampleTable)
    lineTrim(got) shouldBe
      lineTrim(
        """|||||
           |||||
           |||||
           |||||
           |""".stripMargin
      )
  }

  it should "correctly limit" in {
    val got = TableRenderer(TableRenderer.Settings(maxRows = 1)).render(sampleTable)
    lineTrim(got) shouldBe
      lineTrim(
        """||Hello|World |Very\nLong|
           ||-----|------|----------|
           ||How  |Are   |n.A.      |
           |Showing 1 of 2 Rows
           |""".stripMargin
      )
  }

  private def lineTrim(s: String): String = {
    s.linesIterator.map(_.trim).mkString("\n")
  }
}
