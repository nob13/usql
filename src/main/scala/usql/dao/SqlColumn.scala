package usql.dao

import usql.{DataType, SqlIdentifier, SqlRawPart}

/** A Single Column */
case class SqlColumn[T](
    id: SqlIdentifier,
    dataType: DataType[T]
)
