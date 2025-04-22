package usql.dao

import usql.{DataType, ParameterFiller, ResultRowDecoder, SqlIdentifier}

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
  def rowDecoder: ResultRowDecoder[T]

  /** Filler for a full row. */
  def parameterFiller: ParameterFiller[T]
}

object SqlColumnar {
  inline def derived[T <: Product](
      using m: Mirror.ProductOf[T],
      nameMapper: NameMapping = NameMapping.Default
  ): SqlColumnar[T] = {
    SqlFielded.derived[T]
  }
}
