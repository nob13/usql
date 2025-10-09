package usql.dao

import usql.{Query, RowDecoder, Sql, SqlInterpolationParameter, sql}

import java.util.UUID

type ColumnBasePath[T] = ColumnPath[T, T]

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
  def map[R0](f: ColumnBasePath[T] => ColumnPath[T, R0]): QueryBuilder[R0]

  /** Project values. */
  def project[P](p: ColumnPath[T, P]): QueryBuilder[P]

  /** Join two queries. */
  def join[R](right: QueryBuilder[R])(
      on: (ColumnBasePath[T], ColumnBasePath[R]) => Rep[Boolean]
  ): QueryBuilder[(T, R)] = {
    val leftSource  = this.asFromItem()
    val rightSource = right.asFromItem()
    val joinSource  = FromItem.InnerJoin(leftSource, rightSource, on(leftSource.basePath, rightSource.basePath))
    Select.makeSelect(joinSource)
  }

  /** Left Join two Queries */
  def leftJoin[R](right: QueryBuilder[R])(
      on: (ColumnBasePath[T], ColumnBasePath[R]) => Rep[Boolean]
  ): QueryBuilder[(T, Option[R])] = {
    val leftSource  = this.asFromItem()
    val rightSource = right.asFromItem()
    val joinSource  = FromItem.LeftJoin(leftSource, rightSource, on(leftSource.basePath, rightSource.basePath))
    Select.makeSelect(joinSource)
  }

  /** Filter step. */
  def filter(f: ColumnBasePath[T] => Rep[Boolean]): QueryBuilder[T]

  private[usql] def asFromItem(): FromItem[T] = {
    val aliasName = s"X-${UUID.randomUUID()}"
    FromItem.Aliased(FromItem.SubSelect(this), aliasName)
  }

  /** Returns the from item, if this Query is just returning the source. */
  private[usql] def asPureFromItem: Option[FromItem[T]]
}

/** A Query Builder which somehow still presents a projected table. Supports update call. */
trait QueryBuilderForProjectedTable[T] extends QueryBuilder[T] {

  /** Update elements. */
  def update(in: T): Long

  override def map[R0](f: ColumnBasePath[T] => ColumnPath[T, R0]): QueryBuilderForProjectedTable[R0]

  override def project[P](p: ColumnPath[T, P]): QueryBuilderForProjectedTable[P]
}

/** A Query builder which somehow still presents a table. Supports delete call */
trait QueryBuilderForTable[T] extends QueryBuilderForProjectedTable[T] {

  /** Delete selected elements. */
  def delete(): Long

  override def filter(f: ColumnBasePath[T] => Rep[Boolean]): QueryBuilderForTable[T]
}

object QueryBuilder {

  trait CommonMethods[T] extends QueryBuilder[T] {

    /** Returns the base path fore mapping operations. */
    protected def basePath: ColumnBasePath[T]

    override def project[P](p: ColumnPath[T, P]): QueryBuilder[P] = {
      Select(this.asFromItem(), p)
    }

    override def map[R0](f: ColumnBasePath[T] => ColumnPath[T, R0]): QueryBuilder[R0] = {
      project(f(basePath))
    }

    override def filter(f: ColumnBasePath[T] => Rep[Boolean]): QueryBuilder[T] = {
      val from = asFromItem()
      Select(from, from.basePath, Seq(f(from.basePath)))
    }
  }

  def make[T](using tabular: SqlTabular[T]): QueryBuilderForTable[T] = {
    ???
    /*
    val aliasName = s"${tabular.table.name}-${UUID.randomUUID()}" // will be shortened on cleanup
    val from      = FromItem.Aliased(FromItem.FromTable(tabular), aliasName)
    makeSelect(from)
     */
  }
}
