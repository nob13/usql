package usql.dao

import usql.{DataType, SqlIdentifier, SqlIdentifiers, SqlRawPart}

/** A Single Column */
case class SqlColumn[T](
    id: SqlIdentifier,
    dataType: DataType[T]
)

/** Groups muktiple columns. */
case class SqlColumns(
    columns: Seq[SqlColumn[?]]
) {

  /** The count of columns. */
  def count: Int = columns.size

  def map[U](f: SqlColumn[?] => U): Seq[U] = columns.map(f)

  /** Identifiers of all columns */
  def ids: SqlIdentifiers = SqlIdentifiers(map(_.id))

  /** ?-Placeholders for each identifier */
  def placeholders: SqlRawPart = SqlRawPart(map(_.id.placeholder.s).mkString(","))

  /** identifier = ?-Placeholders for Update statements */
  def namedPlaceholders: SqlRawPart = SqlRawPart(map(_.id.namedPlaceholder.s).mkString(","))
}
