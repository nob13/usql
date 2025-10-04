package usql.dao

import usql.SqlColumnId

/** Aliases a table name for use in Join Statements. */
case class Alias[T](
    aliasName: String,
    tabular: SqlTabular[T]
) {

  /** Alias one identifier */
  def apply(c: SqlColumnId): SqlColumnId = {
    c.copy(alias = Some(aliasName))
  }

  /** Refers to all aliased columns */
  def columns: Seq[SqlColumnId] = {
    tabular.columns.map { c =>
      c.id.copy(
        alias = Some(aliasName)
      )
    }
  }

  /** Access to aliased cols. */
  def col: ColumnPath[T, T] = {
    ColumnPath.make(using fielded)
  }

  /** Aliased fielded. */
  def fielded: SqlFielded[T] = tabular.withAlias(aliasName)
}
