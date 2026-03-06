package usql.dao

import usql.SqlInterpolationParameter.SqlParameter
import usql.{Optionalize, RowDecoder, RowEncoder, SqlColumnId}

import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID
import scala.collection.mutable
import scala.deriving.Mirror
import scala.util.NotGiven

/** Something which has fields (e.g. a case class) */
trait SqlFielded[T] extends SqlColumnar[T] with Structure[T] {

  /** Returns the available fields. */
  def fields: Seq[Field[?]]

  override def fieldNames: Seq[String] = fields.map(_.fieldName)

  override def selectField(name: String): Option[Structure[?]] = {
    fields.find(_.fieldName == name).map {
      case c: Field.Column[?] =>
        c.column
      case g: Field.Group[?]  =>
        SqlFielded.MappedSqlFielded(g.fielded, id => g.mapping.map(g.columnBaseName, id))
    }
  }

  /** Access to the columns */
  def cols: ColumnPath[T, T] = ColumnPath.make(using this)

  override protected[dao] def fieldCardinality: Int = fields.size

  override lazy val columns: Seq[SqlColumn[?]] =
    fields.flatMap { field =>
      field.columns
    }

  override def rowDecoder: RowDecoder[T] = new RowDecoder {
    override def parseRow(offset: Int, row: ResultSet): T = {
      val fieldValues   = Seq.newBuilder[Any]
      var currentOffset = offset // scalafix:ok
      fields.foreach { field =>
        fieldValues += field.decoder.parseRow(currentOffset, row)
        currentOffset += field.decoder.cardinality
      }
      build(fieldValues.result())
    }

    override def cardinality: Int = SqlFielded.this.cardinality
  }

  override def rowEncoder: RowEncoder[T] = new RowEncoder[T] {
    override def encode(offset: Int, ps: PreparedStatement, value: T): Unit = {
      var currentOffset = offset // scalafix:ok
      val fieldValues   = split(value)
      fieldValues.zip(fields).foreach { case (fieldValue, field) =>
        field.encoder.fillUnchecked(currentOffset, ps, fieldValue)
        currentOffset += field.encoder.cardinality
      }
    }

    override def cardinality: Int = SqlFielded.this.cardinality

    override def serialize(value: T): Seq[Any] = {
      val fieldValues = split(value)
      fieldValues.zip(fields).flatMap { case (fieldValue, field) =>
        field.encoder.serializeUnchecked(fieldValue)
      }
    }

    override def toSqlParameter(value: T): Seq[SqlParameter[?]] = {
      val builder     = Seq.newBuilder[SqlParameter[?]]
      val fieldValues = split(value)
      fieldValues.zip(fields).foreach { case (fieldValue, field) =>
        builder ++= field.encoder.toSqlParameterUnchecked(fieldValue)
      }
      builder.result()
    }
  }

  override def toString: String = {
    fields.mkString("[", ", ", "]")
  }

  override def optionalize: SqlFielded[Optionalize[T]] = SqlFielded
    .OptionalSqlFielded(this)
    .asInstanceOf[SqlFielded[Optionalize[T]]]

  /** Set an alias. */
  def withAlias(aliasName: String): SqlFielded[T] = {
    SqlFielded.MappedSqlFielded(this, _.copy(alias = Some(aliasName)))
  }

  /** Drops an alias. */
  def dropAlias: SqlFielded[T] = {
    SqlFielded.MappedSqlFielded(this, _.copy(alias = None))
  }

  /**
   * Renames columns, so that all column ids are unique
   * @param keepAlias
   *   if true, keep the alias names.
   */
  def ensureUniqueColumnIds(keepAlias: Boolean): SqlFielded[T] = {
    val columnIds       = columns.map(_.id)
    val inUse           = mutable.Set[SqlColumnId]()
    val resultColumnIds = Seq.newBuilder[SqlColumnId]
    columnIds.foreach { columnId =>
      val baseId  = if keepAlias then {
        columnId
      } else {
        columnId.copy(alias = None)
      }
      val idToUse = if inUse.contains(baseId) then {
        (for
          idx      <- (0 until 32).view
          candidate = baseId.copy(name = baseId.name + idx)
          if !inUse.contains(candidate)
        yield candidate).headOption.getOrElse {
          baseId.copy(name = baseId.name + UUID.randomUUID())
        }
      } else {
        baseId
      }
      inUse += idToUse
      resultColumnIds += idToUse
    }
    SqlFielded.WithColumnsRenamed(this, resultColumnIds.result())
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

    override def isOptional: Boolean = false
  }

  inline def derived[T <: Product: Mirror.ProductOf](using nm: NameMapping = NameMapping.Default): SqlFielded[T] =
    Macros.buildFielded[T]

  case class MappedSqlFielded[T](underlying: SqlFielded[T], mapping: SqlColumnId => SqlColumnId) extends SqlFielded[T] {
    override lazy val fields: Seq[Field[?]] = underlying.fields.map(_.mapColumnNames(mapping))

    override protected[dao] def split(value: T): Seq[Any] = underlying.split(value)

    override protected[dao] def build(fieldValues: Seq[Any]): T = underlying.build(fieldValues)

    override def isOptional: Boolean = underlying.isOptional
  }

  case class OptionalSqlFielded[T](underlying: SqlFielded[T]) extends SqlFielded[Option[T]] {

    val needsOptionalization = underlying.fields.map(f => !f.isOptional)

    override def fields: Seq[Field[?]] = underlying.fields.map {
      case g: Field.Group[?] if g.isOptional =>
        g
      case g: Field.Group[?]                 =>
        g.copy(
          fielded = OptionalSqlFielded(g.fielded)
        )
      case c: Field.Column[?]                =>
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

  /** An SqlFielded with only one instance. */
  case class PseudoFielded[T](c: SqlColumn[T]) extends SqlFielded[T] {
    override def fields: Seq[Field[?]] = Seq(
      Field.Column("", c)
    )

    override protected[dao] def split(value: T): Seq[Any] = {
      Seq(value)
    }

    override protected[dao] def build(fieldValues: Seq[Any]): T = {
      fieldValues.head.asInstanceOf[T]
    }

    override def isOptional: Boolean = {
      c.isOptional
    }
  }

  case class ConcatFielded[L, R](left: SqlFielded[L], right: SqlFielded[R]) extends SqlFielded[(L, R)] {
    override def fields: Seq[Field[?]] = {
      Seq(
        Field.Group("_1", ColumnGroupMapping.Anonymous, SqlColumnId.fromString(""), left),
        Field.Group("_2", ColumnGroupMapping.Anonymous, SqlColumnId.fromString(""), right)
      )
    }

    override protected[dao] def split(value: (L, R)): Seq[Any] = {
      Seq(value._1, value._2)
    }

    override protected[dao] def build(fieldValues: Seq[Any]): (L, R) = {
      (fieldValues.head.asInstanceOf[L], fieldValues(1).asInstanceOf[R])
    }

    override def isOptional: Boolean = {
      false
    }
  }

  case class WithColumnsRenamed[T](base: SqlFielded[T], columnIds: Seq[SqlColumnId]) extends SqlFielded[T] {
    override lazy val fields: Seq[Field[?]] = {
      var remainingColumns = columnIds // scalafix:ok
      val result           = Seq.newBuilder[Field[?]]
      base.fields.foreach {
        case g: Field.Group[?]  =>
          val (fieldColumns, newRemainingColumns) = remainingColumns.splitAt(g.fielded.cardinality)
          result += g.copy(
            mapping = ColumnGroupMapping.Anonymous,
            fielded = WithColumnsRenamed(g.fielded, fieldColumns)
          )
          remainingColumns = newRemainingColumns
        case c: Field.Column[?] =>
          result += c.copy(
            column = c.column.copy(
              id = remainingColumns.head
            )
          )
          remainingColumns = remainingColumns.tail
      }
      result.result()
    }

    override protected[dao] def split(value: T): Seq[Any] = base.split(value)

    override protected[dao] def build(fieldValues: Seq[Any]): T = base.build(fieldValues)

    override def isOptional: Boolean = base.isOptional
  }

  given optionalized[T](using f: SqlFielded[T]): SqlFielded[Optionalize[T]] = f.optionalize

  given asOptional[T](using f: SqlFielded[T], notOption: NotGiven[T <:< Option[?]]): SqlFielded[Option[T]] =
    f.optionalize.asInstanceOf[SqlFielded[Option[T]]]

  given emptyTuple: SqlFielded[EmptyTuple] = SqlFielded.SimpleSqlFielded(
    Nil,
    _ => Nil,
    _ => EmptyTuple
  )

  given recursiveTuple[H, T <: Tuple](using head: Structure[H], tailFielded: SqlFielded[T]): SqlFielded[H *: T] = {
    val headField = head.toField("_1")
    val tailGroup = tailFielded.fields.zipWithIndex.map { case (field, idx) =>
      val updatedName = s"_${idx + 2}"
      field match {
        case c: Field.Column[?] => c.copy(fieldName = updatedName)
        case g: Field.Group[?]  => g.copy(fieldName = updatedName)
      }
    }

    SqlFielded.SimpleSqlFielded[H *: T](
      headField +: tailGroup,
      splitter = x => (head.split(x.head) ++ tailFielded.split(x.tail)).toList,
      builder = x => {
        val (hf, tf) = x.splitAt(head.fieldCardinality)
        head.build(hf) *: tailFielded.build(tf)
      }
    )
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
  def encoder: RowEncoder[T]

  /** The value is optional */
  def isOptional: Boolean

  /** Map all column names. */
  def mapColumnNames(f: SqlColumnId => SqlColumnId): Field[T]
}

object Field {

  /** A Field which maps to a column */
  case class Column[T](fieldName: String, column: SqlColumn[T]) extends Field[T] {
    override def columns: Seq[SqlColumn[?]] = List(column)

    override def decoder: RowDecoder[T] = RowDecoder.forDataType[T](using column.dataType)

    override def encoder: RowEncoder[T] = RowEncoder.forDataType[T](using column.dataType)

    override def toString: String = s"${fieldName}: ($column)"

    override def isOptional: Boolean = column.isOptional

    override def mapColumnNames(f: SqlColumnId => SqlColumnId): Field[T] = {
      copy(
        column = column.copy(
          id = f(column.id)
        )
      )
    }
  }

  /** A Field which maps to a nested case class */
  case class Group[T](
      fieldName: String,
      mapping: ColumnGroupMapping,
      columnBaseName: SqlColumnId,
      fielded: SqlFielded[T]
  ) extends Field[T] {
    override def columns: Seq[SqlColumn[?]] =
      fielded.columns.map { column =>
        column.copy(
          id = mapChildColumnName(column.id)
        )
      }

    def mapChildColumnName(childColumnId: SqlColumnId): SqlColumnId = mapping.map(columnBaseName, childColumnId)

    override def decoder: RowDecoder[T] = fielded.rowDecoder

    override def encoder: RowEncoder[T] = fielded.rowEncoder

    override def toString: String = s"${fieldName}: $fielded"

    override def isOptional: Boolean = fielded.isOptional

    override def mapColumnNames(f: SqlColumnId => SqlColumnId): Field[T] = {
      copy(
        mapping = ColumnGroupMapping.Mapped(mapping, f)
      )
    }
  }
}
