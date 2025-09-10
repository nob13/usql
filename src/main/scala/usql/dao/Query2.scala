package usql.dao

import usql.dao.Query2.{GenericFilter, GenericJoin, GenericLeftJoin, GenericTableProject}
import usql.{ConnectionProvider, Query, Sql, SqlIdentifier, SqlInterpolationParameter, sql}

trait Query2[T] {

  final type BPath    = ColumnPath[T, T]
  final type DPath[X] = ColumnPath[T, X]

  private def basePath: BPath = ColumnPath.make[T](using fielded)

  /** Convert this Query to SQL. */
  def toSql: Sql

  /** Tabular representation of this. */
  def fielded: SqlFielded[T]

  /** Map one element. */
  def map[R0](f: BPath => DPath[R0]): Query2[R0] = project(f(basePath))

  /** Join two queries. */
  def join[R](right: Query2[R])(
      on: (BPath, right.BPath) => Rep[Boolean]
  ): Query2[(T, R)] = GenericJoin(this, right, on(basePath, right.basePath))

  /** Left Join two Queries */
  def leftJoin[R](right: Query2[R])(
      on: (BPath, right.BPath) => Rep[Boolean]
  ): Query2[(T, Option[R])] = GenericLeftJoin(this, right, on(basePath, right.basePath))

  /** Map a tuple element. */
  def map[R0, R1](f0: BPath => DPath[R0], f1: BPath => DPath[R1]): Query2[(R0, R1)] = project(
    CombinedColumnPath(Seq(f0(basePath), f1(basePath)))
  )

  /** Filter step. */
  def filter(f: BPath => Rep[Boolean]): Query2[T] = GenericFilter[T](List(f(basePath)), this)

  /** Project values. */
  def project[R](p: ColumnPath[T, R]): Query2[R] = GenericTableProject(p, this)

  def one()(using cp: ConnectionProvider): Option[T] = {
    Query(toSql).one()(using fielded.rowDecoder)
  }

  def all()(using cp: ConnectionProvider): Vector[T] = {
    Query(toSql).all()(using fielded.rowDecoder)
  }
}

class CombinedColumnPath[R, T <: Tuple](subPaths: Seq[ColumnPath[R, ?]]) extends ColumnPath[R, T] {
  override def selectDynamic(name: String): ColumnPath[R, ?] = ???

  override def buildGetter: R => T = ???

  override def toInterpolationParameter: SqlInterpolationParameter = ???

  override def buildIdentifier: SqlIdentifier = ???

  override def ![X](using ev: T => Option[X]): ColumnPath[R, X] = ???
}

object Query2 {
  def make[T](using tabular: SqlTabular[T]): Query2[T] = {
    TableScan(tabular)
  }

  case class TableScan[T](table: SqlTabular[T]) extends Query2[T] {
    override def toSql: Sql = sql"SELECT ${table.columns} FROM ${table.tableName}"

    override def fielded: SqlFielded[T] = table

    override def project[R](p: ColumnPath[T, R]): Query2[R] = TableProject(p, table)
  }

  case class TableProject[T, B](columnPath: ColumnPath[B, T], table: SqlTabular[B]) extends Query2[T] {
    override def toSql: Sql = sql"SELECT ${columnPath.toInterpolationParameter} FROM ${table.tableName}"

    override def fielded: SqlFielded[T] = ???
  }

  case class GenericTableProject[T, B](columnPath: ColumnPath[B, T], base: Query2[B]) extends Query2[T] {
    // TODO: This needs propably a renaming step.
    override def toSql: Sql = sql"SELECT ${columnPath.toInterpolationParameter} FROM (${base.toSql})"

    override def fielded: SqlFielded[T] = ???

    override def project[R](p: ColumnPath[T, R]): Query2[R] = ???
  }

  case class GenericFilter[T](filters: Seq[Rep[Boolean]] = Nil, base: Query2[T]) extends Query2[T] {
    override def toSql: Sql = ???

    override def fielded: SqlFielded[T] = ???

    override def filter(f: BPath => Rep[Boolean]): Query2[T] = ???
  }

  case class GenericJoin[L, R](left: Query2[L], right: Query2[R], exp: Rep[Boolean]) extends Query2[(L, R)] {
    override def toSql: Sql = ???

    override def fielded: SqlFielded[(L, R)] = ???

  }

  case class GenericLeftJoin[L, R](left: Query2[L], right: Query2[R], exp: Rep[Boolean])
      extends Query2[(L, Option[R])] {
    override def toSql: Sql = ???

    override def fielded: SqlFielded[(L, Option[R])] = ???
  }
}
