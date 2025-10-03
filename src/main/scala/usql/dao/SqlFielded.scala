package usql.dao

import usql.{Optionalize, RowDecoder, RowEncoder, SqlIdentifier}

import java.sql.{PreparedStatement, ResultSet}
import scala.deriving.Mirror

/** Something which has fields (e.g. a case class) */
trait SqlFielded[T] extends SqlColumnar[T] {

  /** Returns the available fields. */
  def fields: Seq[Field[?]]

  /** Access to the columns */
  def cols: ColumnPath[T, T] = ColumnPath.make(using this)

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

  override def toString: String = {
    fields.mkString("[", ", ", "]")
  }

  override def optionalize: SqlFielded[Optionalize[T]] = SqlFielded
    .OptionalSqlFielded(this)
    .asInstanceOf[SqlFielded[Optionalize[T]]]
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

    override def isOptional: Boolean = false
  }

  inline def derived[T <: Product: Mirror.ProductOf](using nm: NameMapping = NameMapping.Default): SqlFielded[T] =
    Macros.buildFielded[T]

  case class MappedSqlFielded[T](underlying: SqlFielded[T], mapping: SqlIdentifier => SqlIdentifier)
      extends SqlFielded[T] {
    override def fields: Seq[Field[_]] = underlying.fields.map {
      case c: Field.Column[?] =>
        c.copy(
          column = c.column.copy(
            id = mapping(c.column.id)
          )
        )
      case g: Field.Group[?]  =>
        g.copy(
          mapping = ColumnGroupMapping.Mapped(g.mapping, mapping)
        )
    }

    override protected[dao] def split(value: T): Seq[Any] = underlying.split(value)

    override protected[dao] def build(fieldValues: Seq[Any]): T = underlying.build(fieldValues)

    override def isOptional: Boolean = underlying.isOptional
  }

  case class OptionalSqlFielded[T](underlying: SqlFielded[T]) extends SqlFielded[Option[T]] {

    val needsOptionalization = underlying.fields.map(f => !f.isOptional)

    override def fields: Seq[Field[_]] = underlying.fields.map {
      case g: Field.Group[?]  =>
        g.copy(
          fielded = OptionalSqlFielded(g.fielded)
        )
      case c: Field.Column[?] =>
        c.copy(
          column = c.column.copy(
            dataType = c.column.dataType.optionalize
          )
        )
    }

    override protected[dao] def split(value: Option[T]): Seq[Any] = {
      value match {
        case None        => Seq.fill(fields.size)(None)
        case Some(value) =>
          underlying.split(value).map(Optionalize.apply)
      }
    }

    override protected[dao] def build(fieldValues: Seq[Any]): Option[T] = {
      if fieldValues == nullValue then {
        None
      } else {
        val unpacked = fieldValues.zip(needsOptionalization).zipWithIndex.map {
          case ((value, true), idx) =>
            value.asInstanceOf[Option[?]].getOrElse {
              throw IllegalArgumentException(s"Unexpected None value in field value ${fields(idx).fieldName}")
            }
          case ((value, _), _)      => value
        }
        Some(underlying.build(unpacked))
      }
    }

    private def nullValue: Seq[Any] = Seq.fill(fields.size)(None)

    override def isOptional: Boolean = true

    override def optionalize: SqlFielded[Optionalize[Option[T]]] = this
  }
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

  /** The value is optional */
  def isOptional: Boolean
}

object Field {

  /** A Field which maps to a column */
  case class Column[T](fieldName: String, column: SqlColumn[T]) extends Field[T] {
    override def columns: Seq[SqlColumn[?]] = List(column)

    override def decoder: RowDecoder[T] = RowDecoder.forDataType[T](using column.dataType)

    override def filler: RowEncoder[T] = RowEncoder.forDataType[T](using column.dataType)

    override def toString: String = s"${fieldName}: ($column)"

    override def isOptional: Boolean = column.isOptional
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
          id = mapChildColumnName(column.id)
        )
      }

    def mapChildColumnName(childColumnId: SqlIdentifier): SqlIdentifier = mapping.map(columnBaseName, childColumnId)

    override def decoder: RowDecoder[T] = fielded.rowDecoder

    override def filler: RowEncoder[T] = fielded.rowEncoder

    override def toString: String = s"${fieldName}: $fielded"

    override def isOptional: Boolean = fielded.isOptional
  }
}
