package usql.dao

import usql.dao.Query2.{FromItem, GenericJoin, GenericLeftJoin}
import usql.{ConnectionProvider, Query, Sql, SqlColumnId, SqlInterpolationParameter, sql}

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
  ): Query2[(T, R)] = {
    val leftSource  = this.asFromItem()
    val rightSource = right.asFromItem()
    GenericJoin(leftSource, rightSource, on(leftSource.basePath, rightSource.basePath))
  }

  /** Left Join two Queries */
  def leftJoin[R](right: Query2[R])(
      on: (BPath, right.BPath) => Rep[Boolean]
  ): Query2[(T, Option[R])] = GenericLeftJoin(this, right, on(basePath, right.basePath))

  /** Filter step. */
  def filter(f: BPath => Rep[Boolean]): Query2[T] = {
    val from = asFromItem()
    Query2.Select(from, from.basePath, Seq(f(from.basePath)))
  }

  /** Project values. */
  def project[P](p: ColumnPath[T, P]): Query2[P] = {
    Query2.Select(asFromItem(), p)
  }

  def one()(using cp: ConnectionProvider): Option[T] = {
    Query(toSql).one()(using fielded.rowDecoder)
  }

  def all()(using cp: ConnectionProvider): Vector[T] = {
    Query(toSql).all()(using fielded.rowDecoder)
  }

  private def asFromItem(): FromItem[T] = {
    val aliasName = s"X-${UUID.randomUUID()}"
    FromItem.Aliased(FromItem.SubSelect(this), aliasName)
  }
}

object Query2 {
  def make[T](using tabular: SqlTabular[T]): Query2[T] = {
    val aliasName = s"${tabular.table}-${UUID.randomUUID()}" // will be shortened on cleanup
    val from      = FromItem.Aliased(FromItem.FromTable(tabular), aliasName)
    Select(from, ColumnPath.make(using from.fielded))
  }

  sealed trait FromItem[T] {
    def fielded: SqlFielded[T]

    def toPreSql: Sql

    lazy val basePath: ColumnPath[T, T] = ColumnPath.make(using fielded)
  }

  object FromItem {
    case class Aliased[T](fromItem: FromItem[T], aliasName: String) extends FromItem[T] {
      override def fielded: SqlFielded[T] = fromItem.fielded.withAlias(aliasName)

      override def toPreSql: Sql = sql"${fromItem.toPreSql} AS ${SqlInterpolationParameter.AliasParameter(aliasName)}"
    }

    case class FromTable[T](tabular: SqlTabular[T]) extends FromItem[T] {
      override def fielded: SqlFielded[T] = tabular

      override def toPreSql: Sql = sql"${tabular.table}"
    }

    case class SubSelect[T](query2: Query2[T]) extends FromItem[T] {
      override def fielded: SqlFielded[T] = query2.fielded.dropAlias

      override def toPreSql: Sql = sql"(${query2.toPreSql})"
    }
    // TODO:
    // - [x] TableIdentifier irgendwie auslagern (das passt mit SqlIdentifier nicht gut, wegen dem Alias)
    // - [x] FromItemSource für eine Tabelle oder ein Query
    // - [x] FromItem selbst mit Source mit potentiellem Alias
    // - [ ] Dynamisches Renaming der Resultat-Werte
    // - [x] TupleColumnPath hat ein funktionierendes Prepend
  }

  case class Select[T, P](from: FromItem[T], projection: ColumnPath[T, P], filters: Seq[Rep[Boolean]] = Nil)
      extends Query2[P] {
    protected override def toPreSql: Sql = {
      val maybeFilterSql: SqlInterpolationParameter = if filters.isEmpty then {
        SqlInterpolationParameter.Empty
      } else {
        sql" WHERE ${filters.reduce(_ && _).toInterpolationParameter}"
      }
      sql"SELECT ${projection.toInterpolationParameter} FROM ${from.toPreSql} ${maybeFilterSql}"
    }

    lazy val fielded: SqlFielded[P] = Query2.ensureFielded(projection.structure)

    override def filter(f: BPath => Rep[Boolean]): Select[T, P] = {
      copy(
        filters = filters :+ f(basePath)
      )
    }

    override def project[P2](p: ColumnPath[P, P2]): Select[T, P2] = {
      copy(
        projection = ColumnPath.concat(projection, p)
      )
    }
  }

  case class GenericJoin[L, R](left: FromItem[L], right: FromItem[R], onExpression: Rep[Boolean])
      extends Query2[(L, R)] {
    protected override def toPreSql: Sql =
      sql"SELECT * FROM ${left.toPreSql} JOIN ${right.toPreSql} ON ${onExpression.toInterpolationParameter}"

    override def fielded: SqlFielded[(L, R)] = SqlFielded.ConcatFielded(left.fielded, right.fielded)

  }

  case class GenericLeftJoin[L, R](left: Query2[L], right: Query2[R], onExpression: Rep[Boolean])
      extends Query2[(L, Option[R])] {
    protected override def toPreSql: Sql =
      sql"SELECT * FROM ${left.toPreSql} LEFT JOIN ${right.toPreSql} ON ${onExpression.toInterpolationParameter}"

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
