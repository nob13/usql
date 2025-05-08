package usql.dao

import usql.{RowEncoder, RowDecoder, SqlIdentifier}

import scala.deriving.Mirror

/** Maps some thing to a whole table */
trait SqlTabular[T] extends SqlFielded[T] {

  /** Name of the table. */
  def tableName: SqlIdentifier

  /** Alias this table to be used in joins */
  def alias(name: String): Alias[T] = Alias(name, this)
}

object SqlTabular {

  /**
   * Derive an instance for a case class.
   *
   * Use [[ColumnName]] to control column names.
   *
   * Use [[TableName]] to control table names.
   *
   * @param nm
   *   name mapping strategy.
   */
  inline def derived[T <: Product: Mirror.ProductOf](using nm: NameMapping = NameMapping.Default): SqlTabular[T] =
    Macros.buildTabular[T]

  case class SimpleTabular[T](
      tableName: SqlIdentifier,
      fielded: SqlFielded[T]
  ) extends SqlTabular[T] {
    override def fields: Seq[Field[?]] = fielded.fields

    override protected[dao] def split(value: T): Seq[Any] = fielded.split(value)

    override protected[dao] def build(fieldValues: Seq[Any]): T = fielded.build(fieldValues)
  }
}
