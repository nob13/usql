package usql.dao

import usql.dao.Query2.{GenericFilter, GenericJoin, GenericLeftJoin, GenericTableProject}
import usql.{ConnectionProvider, Query, Sql, SqlIdentifier, SqlInterpolationParameter, sql}

import java.util.UUID

trait Query2[T] {

  final type BPath    = ColumnPath[T, T]
  final type DPath[X] = ColumnPath[T, X]

  protected def basePath: BPath = ColumnPath.make[T](using fielded)

  /** Convert this Query to SQL. */
  final def toSql: Sql = toPreSql.simplifyAliases

  /** Convert this query to SQL (before End-Optimizations) */
  protected def toPreSql: Sql

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

  /** Filter step. */
  def filter(f: BPath => Rep[Boolean]): Query2[T] = GenericFilter[T](List(f(basePath)), this)

  /** Project values. */
  def project[P](p: ColumnPath[T, P]): Query2[P] = {
    GenericTableProject(p, this, Query2.ensureFielded(p.structure))
  }

  def one()(using cp: ConnectionProvider): Option[T] = {
    Query(toSql).one()(using fielded.rowDecoder)
  }

  def all()(using cp: ConnectionProvider): Vector[T] = {
    Query(toSql).all()(using fielded.rowDecoder)
  }
}

object Query2 {
  def make[T](using tabular: SqlTabular[T]): Query2[T] = {
    val alias = Alias(s"${tabular.tableName}-${UUID.randomUUID()}", tabular)
    TableScan(alias, alias.col)
  }

  case class TableScan[T, P](alias: Alias[T], projection: ColumnPath[T, P], filters: Seq[Rep[Boolean]] = Nil)
      extends Query2[P] {
    protected override def toPreSql: Sql = {
      val maybeFilterSql: SqlInterpolationParameter = if filters.isEmpty then {
        SqlInterpolationParameter.Empty
      } else {
        sql" WHERE ${filters.reduce(_ && _).toInterpolationParameter}"
      }
      sql"SELECT ${projection.toInterpolationParameter} FROM ${alias} ${maybeFilterSql}"
    }

    lazy val fielded: SqlFielded[P] = Query2.ensureFielded(projection.structure)

    override def filter(f: BPath => Rep[Boolean]): TableScan[T, P] = {
      copy(
        filters = filters :+ f(basePath)
      )
    }

    override def project[P2](p: ColumnPath[P, P2]): TableScan[T, P2] = {
      copy(
        projection = ColumnPath.concat(projection, p)
      )
    }
  }

  case class GenericTableProject[T, P](columnPath: ColumnPath[T, P], base: Query2[T], fielded: SqlFielded[P])
      extends Query2[P] {
    protected override def toPreSql: Sql = sql"SELECT ${columnPath.toInterpolationParameter} FROM (${base.toPreSql})"
  }

  case class GenericFilter[T](filters: Seq[Rep[Boolean]] = Nil, base: Query2[T]) extends Query2[T] {

    protected override def toPreSql: Sql = {
      val filtersSql: Rep[Boolean] = filters.reduce(_ && _)
      sql"SELECT * FROM (${base.toPreSql}) WHERE ${filtersSql.toInterpolationParameter}"
    }

    override def fielded: SqlFielded[T] = base.fielded

    override def filter(f: BPath => Rep[Boolean]): Query2[T] = copy(
      filters = filters :+ f(basePath)
    )
  }

  case class GenericJoin[L, R](left: Query2[L], right: Query2[R], exp: Rep[Boolean]) extends Query2[(L, R)] {
    protected override def toPreSql: Sql =
      sql"SELECT * FROM (${left.toPreSql}) JOIN (${right.toPreSql}) ON (${exp.toInterpolationParameter})"

    override def fielded: SqlFielded[(L, R)] = SqlFielded.ConcatFielded(left.fielded, right.fielded)

  }

  case class GenericLeftJoin[L, R](left: Query2[L], right: Query2[R], exp: Rep[Boolean])
      extends Query2[(L, Option[R])] {
    protected override def toPreSql: Sql =
      sql"SELECT * FROM (${left.toPreSql}) LEFT JOIN (${right.toPreSql}) ON (${exp.toInterpolationParameter})"

    override def fielded: SqlFielded[(L, Option[R])] =
      SqlFielded.ConcatFielded(left.fielded, SqlFielded.OptionalSqlFielded(right.fielded))
  }

  private def ensureFielded[T](in: SqlColumn[T] | SqlFielded[T]): SqlFielded[T] = {
    in match {
      case f: SqlFielded[T] => f
      case c: SqlColumn[T]  => SqlFielded.PseudoFielded(c)
    }
  }
}
