package usql.dao

import usql.{SqlIdentifier, SqlRawPart}

/** Experimental helper for building aliases used in Join Statements */
case class Alias[T](
    aliasName: String,
    tabular: SqlTabular[T]
) {

  /** Alias one identifier */
  def apply(c: SqlIdentifier): SqlRawPart = {
    SqlRawPart(this.aliasName + "." + c.serialize)
  }

  /** Refers to all aliased columns */
  def columns: SqlRawPart = {
    SqlRawPart(
      tabular.columns
        .map { c => apply(c.id).s }
        .mkString(",")
    )
  }

  /** Access to aliased cols. */
  def col: ColumnPath[T, T] = ColumnPath(tabular, Nil, alias = Some(aliasName))
}
