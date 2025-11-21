package usql.comparison

/**
 * A simple table which can be rendered in ASCII. All cells are optional.
 */
case class Table(
    columns: IndexedSeq[String],
    rows: IndexedSeq[IndexedSeq[Option[String]]]
) {

  def columnCount: Int = columns.size

  def isEmpty: Boolean = columns.isEmpty || rows.isEmpty

  /** Normalizes the length of all rows to the column count. */
  def normalized: Table = {
    val columnCount    = columns.size
    val normalizedRows = rows.view
      .map {
        case row if row.size == columnCount => row
        case row if row.size < columnCount  => row ++ Seq.fill(columnCount - row.size)(None)
        case row                            => row.take(columnCount)
      }
      .map(_.toIndexedSeq)
      .toIndexedSeq
    Table(columns, normalizedRows)
  }

  def getByName(columnName: String, rowId: Int): Option[String] = {
    if rows.isDefinedAt(rowId) then {
      val row = rows(rowId)
      columns.indexOf(columnName) match {
        case n if row.isDefinedAt(n) =>
          row(n)
        case _                       => None
      }
    } else {
      None
    }

  }

  override def toString: String = {
    TableRenderer.default.renderPretty(this)
  }
}
