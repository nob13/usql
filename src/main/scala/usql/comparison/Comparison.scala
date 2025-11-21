package usql.comparison

/** Comparison result for something about T. */
trait Comparison[T] {
  def isSuccess: Boolean

  def isFailure: Boolean = !isSuccess

  def renderFailure(settings: TableRenderer.Settings = TableRenderer.Settings())(using ToTable[T]): Option[String]
}

/** Compares that two sets have the same field values. */
case class SameElementComparison[T](
    unexpected: Seq[T],
    missing: Seq[T],
    matching: Seq[T]
) extends Comparison[T] {
  override def isSuccess: Boolean = unexpected.isEmpty && missing.isEmpty

  override def renderFailure(settings: TableRenderer.Settings)(using t: ToTable[T]): Option[String] = {
    val renderer = TableRenderer(settings)
    if isSuccess then {
      None
    } else {
      val sb = StringBuilder()
      sb ++= s"Matching: ${matching.size} Elements\n"
      if unexpected.nonEmpty then {
        sb ++= "Unexpected: "
        renderer.renderPrettyTo(Table.from(unexpected), sb)
        sb += '\n'
      }
      if missing.nonEmpty then {
        sb ++= "Missing:    "
        renderer.renderPrettyTo(Table.from(missing), sb)
        sb += '\n'
      }
      if missing.size == 1 && unexpected.size == 1 then {
        val missingTable    = Table.from(missing)
        val unexpectedTable = Table.from(unexpected)
        if missingTable.columnCount > 1 && unexpectedTable.columnCount > 1 then {
          val keys            = (missingTable.columns ++ unexpectedTable.columns).distinct
          val rows            = for
            key     <- keys
            expected = missingTable.getByName(key, 0).getOrElse(settings.noneField)
            got      = unexpectedTable.getByName(key, 0).getOrElse(settings.noneField)
            if expected != got
          yield {
            IndexedSeq(key, expected, got)
          }
          val comparisonTable = Table(IndexedSeq("Key", "Expected", "Got"), rows)
          sb ++= s"Comparison: "
          renderer.renderPrettyTo(comparisonTable, sb)
          sb += '\n'
        }
      }
      Some(sb.result())
    }
  }
}

object SameElementComparison {

  /** Compare two sequences for containing the same elements (order not important) */
  def compare[T](got: Seq[T], expected: Seq[T]): SameElementComparison[T] = {
    val unexpected = got.diff(expected)
    val missing    = expected.diff(got)
    val matching   = got.intersect(expected)
    SameElementComparison(
      unexpected = unexpected,
      missing = missing,
      matching = matching
    )
  }
}
