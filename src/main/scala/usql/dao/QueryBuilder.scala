package usql.dao

import usql.{ConnectionProvider, Query, RowDecoder, Sql, SqlInterpolationParameter, sql}

import java.util.UUID

type ColumnBasePath[T] = ColumnPath[?, T]

/** A Query Builder based upon filter, map and join methods. */
trait QueryBuilder[T] extends Query[T] {

  /** Convert this query to SQL. */
  final def sql: Sql = toPreSql.simplifyAliases

  override def rowDecoder: RowDecoder[T] = fielded.rowDecoder

  /** Convert this query to SQL (before End-Optimizations) */
  private[usql] def toPreSql: Sql

  /** Tabular representation of the result. */
  def fielded: SqlFielded[T]

  /** Map one element. */
  def map[R0](f: ColumnPath[T, T] => ColumnPath[T, R0]): QueryBuilder[R0]

  /** Project values. */
  def project[P](p: ColumnPath[T, P]): QueryBuilder[P]

  /** Filter step. */
  def filter(f: ColumnBasePath[T] => Rep[Boolean]): QueryBuilder[T]

  /** Join two queries. */
  def join[R](right: QueryBuilder[R])(on: (ColumnBasePath[T], ColumnBasePath[R]) => Rep[Boolean]): QueryBuilder[(T, R)]

  /** Left Join two Queries */
  def leftJoin[R](right: QueryBuilder[R])(
      on: (ColumnBasePath[T], ColumnBasePath[R]) => Rep[Boolean]
  ): QueryBuilder[(T, Option[R])]

  /** Converts into a From Item which has an alias. */
  private[usql] def asAliasedFromItem(): FromItem[T] = {
    val aliasName = s"X-${UUID.randomUUID()}"
    FromItem.Aliased(FromItem.SubSelect(this), aliasName)
  }
}

/** A Query Builder which somehow still presents a projected table. Supports update call. */
trait QueryBuilderForProjectedTable[T] extends QueryBuilder[T] {

  /** Update elements. */
  def update(in: T)(using cp: ConnectionProvider): Int

  override def map[R0](f: ColumnPath[T, T] => ColumnPath[T, R0]): QueryBuilderForProjectedTable[R0]

  override def project[P](p: ColumnPath[T, P]): QueryBuilderForProjectedTable[P]
}

/** A Query builder which somehow still presents a table. Supports delete call */
trait QueryBuilderForTable[T] extends QueryBuilderForProjectedTable[T] {

  /** Delete selected elements. */
  def delete()(using cp: ConnectionProvider): Int

  override def filter(f: ColumnBasePath[T] => Rep[Boolean]): QueryBuilderForTable[T]
}

object QueryBuilder {

  def make[T](using tabular: SqlTabular[T]): QueryBuilderForTable[T] = {
    SimpleTableSelect(tabular)
  }
}
