package usql.dao

import usql.{Optionalize, RowDecoder, RowEncoder}

import scala.deriving.Mirror

/**
 * Encapsulates column data and codecs for a product type.
 *
 * Note: for case classes, this is usually presented by [[SqlFielded]]
 */
trait SqlColumnar[T] {

  /** The columns */
  def columns: Seq[SqlColumn[?]]

  /** Count of columns */
  def cardinality: Int = columns.size

  /** Decoder for a full row. */
  def rowDecoder: RowDecoder[T]

  /** Filler for a full row. */
  def rowEncoder: RowEncoder[T]

  /** Returns true if T is an optional type */
  def isOptional: Boolean

  /** Returns the optional variant. */
  def optionalize: SqlColumnar[Optionalize[T]]
}
