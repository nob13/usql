package usql.dao

import usql.{DataType, Optionalize, RowDecoder, RowEncoder, SqlColumnId, SqlRawPart}

/** A Single Column */
case class SqlColumn[T](
    id: SqlColumnId,
    dataType: DataType[T]
) extends SqlColumnar[T] {
  override def columns: Seq[SqlColumn[_]] = List(this)

  override def rowDecoder: RowDecoder[T] = RowDecoder.forDataType(using dataType)

  override def rowEncoder: RowEncoder[T] = RowEncoder.forDataType(using dataType)

  override def toString: String = {
    s"${id}: ${dataType}"
  }

  override def isOptional: Boolean = dataType.isOptional

  override def optionalize: SqlColumn[Optionalize[T]] = copy(
    dataType = dataType.optionalize
  )
}
