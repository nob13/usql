package usql.dao

import usql.{DataType, SqlIdentifier}

/** Something which has fields (e.g. a case class) */
trait SqlFielded[T] extends SqlColumnar[T] {

  /** Returns the available fields. */
  def fields: Field[?]

}

/** A Field of a case class. */
sealed trait Field[T] {
  def fieldName: String
}

object Field {

  /** A Field which maps to a column */
  case class Column[T](fieldName: String, columnName: SqlIdentifier, dataType: DataType[T]) extends Field[T]

  /** A Field which maps to a nested case class */
  case class Group[T](fieldName: String, fielded: SqlFielded[T]) extends Field[T]
}
