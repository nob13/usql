package usql.dao

/** Something which has fields (e.g. a case class) */
trait SqlFielded[T] extends SqlColumnar[T] {

  /** Returns the available fields. */
  def fields: Seq[Field[?]]

  override def columns: SqlColumns = SqlColumns {
    fields.flatMap { field =>
      field.columns.columns
    }
  }
}

/** A Field of a case class. */
sealed trait Field[T] {

  /** Name of the field (case class member) */
  def fieldName: String

  /** Columns represented by this field. */
  def columns: SqlColumns
}

object Field {

  /** A Field which maps to a column */
  case class Column[T](fieldName: String, column: SqlColumn[T]) extends Field[T] {
    override def columns: SqlColumns = SqlColumns(List(column))
  }

  /** A Field which maps to a nested case class */
  case class Group[T](fieldName: String, fielded: SqlFielded[T]) extends Field[T] {
    override def columns: SqlColumns = fielded.columns
  }
}
