package usql.dao

import usql.{DataType, Optionalize, RowDecoder, RowEncoder, SqlColumnId}

/** A Single Column */
case class SqlColumn[T](
    id: SqlColumnId,
    dataType: DataType[T]
) extends SqlColumnar[T]
    with Structure[T] {
  override def columns: Seq[SqlColumn[?]] = List(this)

  override def rowDecoder: RowDecoder[T] = RowDecoder.forDataType(using dataType)

  override def rowEncoder: RowEncoder[T] = RowEncoder.forDataType(using dataType)

  override def toString: String = {
    s"${id}: ${dataType}"
  }

  override def isOptional: Boolean = dataType.isOptional

  override def optionalize: SqlColumn[Optionalize[T]] = copy(
    dataType = dataType.optionalize
  )

  override def selectField(name: String): Option[Structure[?]] = None

  override def fieldNames: Seq[String] = Nil

  override def toField(fieldName: String): Field[T] = Field.Column(fieldName, this)

  override protected[dao] def split(value: T): Seq[Any] = Seq(value)

  override protected[dao] def build(fieldValues: Seq[Any]): T = fieldValues.head.asInstanceOf[T]

  override protected[dao] def fieldCardinality: Int = 1
}

object SqlColumn {

  def apply[T](name: String, dataType: DataType[T]): SqlColumn[T] = SqlColumn(SqlColumnId.fromString(name), dataType)
}
