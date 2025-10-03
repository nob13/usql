package usql.dao

import usql.{DataType, RowDecoder, RowEncoder, SqlIdentifier, SqlRawPart}

/** A Single Column */
case class SqlColumn[T](
    id: SqlIdentifier,
    dataType: DataType[T]
) extends SqlColumnar[T] {
  override def columns: Seq[SqlColumn[_]] = List(this)

  override def rowDecoder: RowDecoder[T] = RowDecoder.forDataType(using dataType)

  override def rowEncoder: RowEncoder[T] = RowEncoder.forDataType(using dataType)

  override def toString: String = {
    s"${id}: ${dataType}"
  }
}
