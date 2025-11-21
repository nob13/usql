package usql.comparison
import TableRenderer.Settings

object TableRenderer {

  case class Settings(
      maxCellWidth: Int = 128,
      noneField: String = "n.A.",
      tooLongSuffix: String = "...",
      columnSeparator: Char = '|',
      rowSeparator: Char = '-',
      flatIfPossible: Boolean = false,
      maxRows: Int = 50
  )

  def default: TableRenderer = TableRenderer(Settings())
}

/** Simple ASCII Table Renderer. */
case class TableRenderer(settings: Settings) {

  /** Render as a table. */
  def render(table: Table): String = {
    val normalized = table.normalized
    val widths     = calcColumnWidths(normalized)
    renderNormalized(normalized, widths)
  }

  /**
   * Render something as flat if possible, as table otherwise. A newline is inserted if it's rendered as table.
   */
  def renderPretty(table: Table): String = {
    if table.isEmpty then {
      "<Empty>"
    } else {
      if table.columns.size <= 1 then {
        renderFlat(table)
      } else {
        "\n" + render(table)
      }
    }
  }

  /**
   * Render only the first column flat, if existing.
   */
  def renderFlat(table: Table): String = {
    if table.columns.isEmpty then {
      return "<Empty>"
    }
    if table.rows.isEmpty then {
      return "<Empty>"
    }
    table.rows
      .map { row =>
        val woNewline = withoutNewline(row.headOption.getOrElse(settings.noneField))
        trimToLength(woNewline, settings.maxCellWidth)
      }
      .mkString(",")
  }

  private def calcColumnWidths(normalized: Table): Seq[Int] = {
    val headerWidths: Seq[Int] = normalized.columns.view.map(withoutNewline(_).length).toSeq
    normalized.rows
      .foldLeft(headerWidths) { case (c, row) =>
        val cellWidths = row.view.map { cell =>
          val cellValue = withoutNewline(cell)
          cellValue.length
        }
        c.zip(cellWidths).map { case (a, b) => Math.max(a, b) }
      }
      .map(Math.min(_, settings.maxCellWidth))
  }

  private def renderNormalized(normalized: Table, columnWidths: Seq[Int]): String = {
    val sb         = StringBuilder()
    var first      = true
    val fullLength = columnWidths.sum + normalized.columns.size - 1
    normalized.columns.view.zip(columnWidths).foreach { case (header, len) =>
      if first then {
        sb += settings.columnSeparator
      }
      first = false
      sb ++= formatCell(header, len)
      sb += settings.columnSeparator
    }
    sb += '\n'

    first = true
    columnWidths.foreach { width =>
      if first then {
        sb += settings.columnSeparator
      }
      first = false
      for i <- 0 until width do {
        sb += settings.rowSeparator
      }
      sb += settings.columnSeparator
    }
    sb += '\n'

    normalized.rows.view.take(settings.maxRows).foreach { row =>
      first = true
      row.view.zip(columnWidths).foreach { case (cell, len) =>
        if first then {
          sb += settings.columnSeparator
        }
        first = false
        sb ++= formatCell(cell, len)
        sb += settings.columnSeparator
      }
      sb += '\n'
    }

    if normalized.rows.size >= settings.maxRows then {
      sb ++= s"Showing ${settings.maxRows} of ${normalized.rows.size} Rows\n"
    }

    sb.result()
  }

  private def formatCell(s: String, fixLength: Int): String = {
    val candidate = withoutNewline(s)
    if candidate.length > fixLength then {
      trimToLength(s, fixLength)
    } else {
      candidate + (" " * (fixLength - candidate.length))
    }
  }

  private def withoutNewline(s: String): String = {
    s.replace("\n", "\\n")
  }

  private def trimToLength(s: String, length: Int): String = {
    if s.length > length then {
      (s.take(length - settings.tooLongSuffix.length) + settings.tooLongSuffix).take(length)
    } else {
      s
    }
  }
}
