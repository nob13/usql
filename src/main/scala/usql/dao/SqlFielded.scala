package usql.dao

import usql.{ParameterFiller, ResultRowDecoder}

import java.sql.{PreparedStatement, ResultSet}
import scala.deriving.Mirror

/** Something which has fields (e.g. a case class) */
trait SqlFielded[T] extends SqlColumnar[T] {

  /** Returns the available fields. */
  def fields: Seq[Field[?]]

  /** Split an instance into its fields */
  protected def split(value: T): Seq[Any]

  /** Build from field values. */
  protected def build(fieldValues: Seq[Any]): T

  override lazy val columns: SqlColumns = SqlColumns {
    fields.flatMap { field =>
      field.columns.columns
    }
  }

  override def rowDecoder: ResultRowDecoder[T] = new ResultRowDecoder {
    override def parseRow(offset: Int, row: ResultSet): T = {
      val fieldValues   = Seq.newBuilder[Any]
      var currentOffset = offset
      fields.foreach { field =>
        fieldValues += field.decoder.parseRow(currentOffset, row)
        currentOffset += field.decoder.cardinality
      }
      build(fieldValues.result())
    }

    override def cardinality: Int = SqlFielded.this.cardinality
  }

  override def parameterFiller: ParameterFiller[T] = new ParameterFiller[T] {
    override def fill(offset: Int, ps: PreparedStatement, value: T): Unit = {
      var currentOffset = offset
      val fieldValues   = split(value)
      fieldValues.zip(fields).foreach { case (fieldValue, field) =>
        field.filler.fillUnchecked(currentOffset, ps, fieldValue)
        currentOffset += field.filler.cardinality
      }
    }

    override def cardinality: Int = SqlFielded.this.cardinality
  }
}

object SqlFielded {

  /** Simple implementation. */
  case class SimpleSqlFielded[T](
      fields: Seq[Field[?]],
      splitter: T => List[Any],
      builder: List[Any] => T
  ) extends SqlFielded[T] {
    override protected def split(value: T): Seq[Any] = splitter(value)

    override protected def build(fieldValues: Seq[Any]): T = builder(fieldValues.toList)
  }

  inline def derived[T <: Product: Mirror.ProductOf](using nm: NameMapping = NameMapping.Default): SqlFielded[T] =
    Macros.buildFielded[T]

}

/** A Field of a case class. */
sealed trait Field[T] {

  /** Name of the field (case class member) */
  def fieldName: String

  /** Columns represented by this field. */
  def columns: SqlColumns

  /** Decoder for this field. */
  def decoder: ResultRowDecoder[T]

  /** Filler for this field. */
  def filler: ParameterFiller[T]
}

object Field {

  /** A Field which maps to a column */
  case class Column[T](fieldName: String, column: SqlColumn[T]) extends Field[T] {
    override def columns: SqlColumns = SqlColumns(List(column))

    override def decoder: ResultRowDecoder[T] = ResultRowDecoder.forDataType[T](using column.dataType)

    override def filler: ParameterFiller[T] = ParameterFiller.forDataType[T](using column.dataType)
  }

  /** A Field which maps to a nested case class */
  case class Group[T](fieldName: String, fielded: SqlFielded[T]) extends Field[T] {
    override def columns: SqlColumns = fielded.columns

    override def decoder: ResultRowDecoder[T] = fielded.rowDecoder

    override def filler: ParameterFiller[T] = fielded.parameterFiller
  }
}
