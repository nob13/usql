package usql.dao

import usql.{RowEncoder, RowDecoder}

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
}
