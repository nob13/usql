package usql.comparison

import usql.DataType
import usql.dao.SqlColumnar

/**
 * A simple table which can be rendered in ASCII.
 */
case class Table(
    columns: Seq[String],
    rows: Seq[Seq[String]]
) {

  def columnCount: Int = columns.size

  def isEmpty: Boolean = columns.isEmpty || rows.isEmpty

  /** Normalizes the length of all rows to the column count. */
  def normalized: Table = {
    val columnCount    = columns.size
    val normalizedRows = rows.view
      .map {
        case row if row.size == columnCount => row
        case row if row.size < columnCount  => row ++ Seq.fill(columnCount - row.size)("")
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
        case n if row.isDefinedAt(n) => Some(row(n))
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

/** Type class converting something to a table. */
trait ToTable[T] {

  /** Converts a values to a table. */
  def toTable(values: Seq[T]): Table
}

/** To-Table conversion, where always the same columns for a type are returned. */
trait StaticToTable[T] extends ToTable[T] {

  /** Column Names. */
  def columns: Seq[String]

  /** Row values. */
  def rows(values: Seq[T]): Seq[Seq[String]]

  final def toTable(values: Seq[T]): Table = Table(columns, rows(values))
}

object ToTable {
  given columnarToTable[T](using columnar: SqlColumnar[T]): StaticToTable[T] with {
    val columns           = columnar.columns.map(_.id.name).toIndexedSeq
    private val dataTypes = columnar.columns.map(_.dataType)
    private val encoder   = columnar.rowEncoder

    override def rows(values: Seq[T]): IndexedSeq[IndexedSeq[String]] = {
      values.map { value =>
        encoder
          .serialize(value)
          .zip(dataTypes)
          .map { case (value, dataType) =>
            uncheckedToString(dataType, value)
          }
          .toVector
      }.toVector
    }

    private def uncheckedToString[X](dataType: DataType[X], value: Any): String = {
      dataType.serialize(value.asInstanceOf[X])
    }
  }
}

object Table {

  /** Converts elements into a table. */
  def from[T](values: Seq[T])(using c: ToTable[T]): Table = c.toTable(values)
}
