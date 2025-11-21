package usql.comparison

class TableRendererTest extends ComparisonTestBase {

  val sampleTable = Table(
    columns = Seq("Hello", "World", "Very\nLong"),
    rows = Seq(
      Seq("How", "Are", ""),
      Seq("", "You\n?")
    )
  )

  "render" should "work per by default" in {
    lineTrim(TableRenderer.default.render(sampleTable)) shouldBe
      lineTrim(
        """||Hello|World |Very\nLong|
           ||-----|------|----------|
           ||How  |Are   |          |
           ||     |You\n?|          |
           |""".stripMargin
      )
  }

  it should "work with short max size" in {
    val got = TableRenderer(TableRenderer.Settings(maxCellWidth = 4)).render(sampleTable)
    lineTrim(got) shouldBe
      lineTrim(
        """||H...|W...|V...|
           ||----|----|----|
           ||How |Are |    |
           ||    |Y...|    |
           |""".stripMargin
      )
  }

  it should "not crash on absurd small values" in {
    val got = TableRenderer(TableRenderer.Settings(maxCellWidth = 1)).render(sampleTable)
    lineTrim(got) shouldBe
      lineTrim(
        """||.|.|.|
           ||-|-|-|
           ||.|.| |
           || |.| |
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
           ||How  |Are   |          |
           |Showing 1 of 2 Rows
           |""".stripMargin
      )
  }
}
