package usql.dao

import usql.{RowEncoder, RowDecoder, SqlIdentifier}

import java.sql.{PreparedStatement, ResultSet}
import scala.deriving.Mirror

/** Something which has fields (e.g. a case class) */
trait SqlFielded[T] extends SqlColumnar[T] {

  /** Returns the available fields. */
  def fields: Seq[Field[?]]

  /** Access to the columns */
  def cols: ColumnPath[T, T] = ColumnPath(this, Nil)

  /** Split an instance into its fields */
  protected[dao] def split(value: T): Seq[Any]

  /** Build from field values. */
  protected[dao] def build(fieldValues: Seq[Any]): T

  override lazy val columns: Seq[SqlColumn[?]] =
    fields.flatMap { field =>
      field.columns
    }

  override def rowDecoder: RowDecoder[T] = new RowDecoder {
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

  override def rowEncoder: RowEncoder[T] = new RowEncoder[T] {
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
    override protected[dao] def split(value: T): Seq[Any] = splitter(value)

    override protected[dao] def build(fieldValues: Seq[Any]): T = builder(fieldValues.toList)
  }

  inline def derived[T <: Product: Mirror.ProductOf](using nm: NameMapping = NameMapping.Default): SqlFielded[T] =
    Macros.buildFielded[T]

}

/** A Field of a case class. */
sealed trait Field[T] {

  /** Name of the field (case class member) */
  def fieldName: String

  /** Columns represented by this field. */
  def columns: Seq[SqlColumn[?]]

  /** Decoder for this field. */
  def decoder: RowDecoder[T]

  /** Filler for this field. */
  def filler: RowEncoder[T]
}

object Field {

  /** A Field which maps to a column */
  case class Column[T](fieldName: String, column: SqlColumn[T]) extends Field[T] {
    override def columns: Seq[SqlColumn[?]] = List(column)

    override def decoder: RowDecoder[T] = RowDecoder.forDataType[T](using column.dataType)

    override def filler: RowEncoder[T] = RowEncoder.forDataType[T](using column.dataType)
  }

  /** A Field which maps to a nested case class */
  case class Group[T](
      fieldName: String,
      mapping: ColumnGroupMapping,
      columnBaseName: SqlIdentifier,
      fielded: SqlFielded[T]
  ) extends Field[T] {
    override def columns: Seq[SqlColumn[?]] =
      fielded.columns.map { column =>
        column.copy(
          id = mapping.map(columnBaseName, column.id)
        )
      }

    override def decoder: RowDecoder[T] = fielded.rowDecoder

    override def filler: RowEncoder[T] = fielded.rowEncoder
  }
}
