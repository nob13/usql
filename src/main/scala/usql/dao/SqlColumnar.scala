package usql.dao

import usql.{DataType, ParameterFiller, ResultRowDecoder, SqlIdentifier}

import scala.deriving.Mirror

/** Encapsulates column data and codecs for a product type */
trait SqlColumnar[T] {

  /** The columns */
  def columns: SqlColumns
  
  /** Count of columns */
  def cardinality: Int = columns.count

  /** Decoder for a full row. */
  def rowDecoder: ResultRowDecoder[T]

  /** Filler for a full row. */
  def parameterFiller: ParameterFiller[T]
}

object SqlColumnar {

  /**
   * Derive an instance for a case class.
   *
   * Use [[ColumnName]] to control column names.
   *
   * Use [[ColumnGroup]] to control column names of nested column groups.
   *
   * @param nm
   *   name mapping strategy.
   */
  inline def derived[T <: Product: Mirror.ProductOf](using nm: NameMapping = NameMapping.Default): SqlColumnar[T] =
    Macros.buildColumnar[T]

  case class SimpleColumnar[T](
      columns: SqlColumns,
      rowDecoder: ResultRowDecoder[T],
      parameterFiller: ParameterFiller[T]
  ) extends SqlColumnar[T]
}
