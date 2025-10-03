package usql.dao

import usql.{SqlIdentifier, SqlRawPart}

/** Aliases a table name for use in Join Statements. */
case class Alias[T](
    aliasName: String,
    tabular: SqlTabular[T]
) {

  /** Alias one identifier */
  def apply(c: SqlIdentifier): SqlRawPart = {
    SqlRawPart(this.aliasName + "." + c.serialize)
  }

  /** Refers to all aliased columns */
  def columns: Seq[SqlIdentifier] = {
    tabular.columns.map { c =>
      c.id.copy(
        alias = Some(aliasName)
      )
    }
  }

  /** Access to aliased cols. */
  def col: ColumnPath[T, T] = {
    tabular.cols.withAlias(aliasName)
  }

  /** Aliased fielded. */
  def fielded: SqlFielded[T] = SqlFielded.MappedSqlFielded(tabular, id => id.copy(alias = Some(aliasName)))
}
