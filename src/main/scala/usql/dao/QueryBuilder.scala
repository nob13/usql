package usql.dao

import usql.dao.QueryBuilder.{FromItem, makeSelect}
import usql.{Query, RowDecoder, Sql, SqlInterpolationParameter, sql}

import java.util.UUID

/** A Query Builder based upon filter, map and join methods. */
trait QueryBuilder[T] extends Query[T] {

  final type BPath = ColumnPath[T, T]

  /** Returns the base path fore mapping operations. */
  protected def basePath: BPath

  final def sql: Sql = toPreSql.simplifyAliases

  override def rowDecoder: RowDecoder[T] = fielded.rowDecoder

  /** Convert this query to SQL (before End-Optimizations) */
  protected def toPreSql: Sql

  /** Tabular representation of the result. */
  def fielded: SqlFielded[T]

  /** Map one element. */
  def map[R0](f: BPath => ColumnPath[T, R0]): QueryBuilder[R0] = project(f(basePath))

  /** Join two queries. */
  def join[R](right: QueryBuilder[R])(
      on: (BPath, right.BPath) => Rep[Boolean]
  ): QueryBuilder[(T, R)] = {
    val leftSource  = this.asFromItem()
    val rightSource = right.asFromItem()
    val joinSource  = FromItem.InnerJoin(leftSource, rightSource, on(leftSource.basePath, rightSource.basePath))
    makeSelect(joinSource)
  }

  /** Left Join two Queries */
  def leftJoin[R](right: QueryBuilder[R])(
      on: (BPath, right.BPath) => Rep[Boolean]
  ): QueryBuilder[(T, Option[R])] = {
    val leftSource  = this.asFromItem()
    val rightSource = right.asFromItem()
    val joinSource  = FromItem.LeftJoin(leftSource, rightSource, on(leftSource.basePath, rightSource.basePath))
    makeSelect(joinSource)
  }

  /** Filter step. */
  def filter(f: BPath => Rep[Boolean]): QueryBuilder[T] = {
    val from = asFromItem()
    QueryBuilder.Select(from, from.basePath, Seq(f(from.basePath)))
  }

  /** Project values. */
  def project[P](p: ColumnPath[T, P]): QueryBuilder[P] = {
    QueryBuilder.Select(asFromItem(), p)
  }

  private def asFromItem(): FromItem[T] = {
    val aliasName = s"X-${UUID.randomUUID()}"
    FromItem.Aliased(FromItem.SubSelect(this), aliasName)
  }

  /** Returns the from item, if this Query is just returning the source. */
  def asPureFromItem: Option[FromItem[T]]
}

object QueryBuilder {
  def make[T](using tabular: SqlTabular[T]): QueryBuilder[T] = {
    val aliasName = s"${tabular.table.name}-${UUID.randomUUID()}" // will be shortened on cleanup
    val from      = FromItem.Aliased(FromItem.FromTable(tabular), aliasName)
    makeSelect(from)
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

    case class SubSelect[T](query2: QueryBuilder[T]) extends FromItem[T] {
      override def fielded: SqlFielded[T] = query2.fielded.dropAlias

      override def toPreSql: Sql = sql"(${query2.toPreSql})"
    }

    case class InnerJoin[L, R](left: FromItem[L], right: FromItem[R], onExpression: Rep[Boolean])
        extends FromItem[(L, R)] {
      override def fielded: SqlFielded[(L, R)] = SqlFielded.ConcatFielded(left.fielded, right.fielded)

      override def toPreSql: Sql = {
        sql"${left.toPreSql} JOIN ${right.toPreSql} ON ${onExpression.toInterpolationParameter}"
      }
    }

    case class LeftJoin[L, R](left: FromItem[L], right: FromItem[R], onExpression: Rep[Boolean])
        extends FromItem[(L, Option[R])] {
      override def fielded: SqlFielded[(L, Option[R])] =
        SqlFielded.ConcatFielded(left.fielded, SqlFielded.OptionalSqlFielded(right.fielded))

      override def toPreSql: Sql = {
        sql"${left.toPreSql} LEFT JOIN ${right.toPreSql} ON ${onExpression.toInterpolationParameter}"
      }
    }
  }

  case class Select[T, P](from: FromItem[T], projection: ColumnPath[T, P], filters: Seq[Rep[Boolean]] = Nil)
      extends QueryBuilder[P] {
    protected override def toPreSql: Sql = {
      val maybeFilterSql: SqlInterpolationParameter = if filters.isEmpty then {
        SqlInterpolationParameter.Empty
      } else {
        sql" WHERE ${filters.reduce(_ && _).toInterpolationParameter}"
      }
      val projectionString                          = SqlInterpolationParameter.MultipleSeparated(
        projection.columnIds.zip(fielded.columns.map(_.id)).map { case (p, as) =>
          sql"${p} AS ${as}"
        }
      )
      sql"SELECT ${projectionString} FROM ${from.toPreSql} ${maybeFilterSql}"
    }

    /** The fielded representation from the outside. */
    lazy val fielded: SqlFielded[P] = {
      innerFielded.ensureUniqueColumnIds(keepAlias = false)
    }

    /** The fielded representation inside (using aliases) */
    lazy val innerFielded: SqlFielded[P] = {
      QueryBuilder.ensureFielded(projection.structure)
    }

    override protected def basePath: BPath = {
      ColumnPath.make[P](using innerFielded)
    }

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

    override def asPureFromItem: Option[FromItem[P]] = {
      Option.when(projection.isEmpty && filters.isEmpty) {
        from.asInstanceOf[FromItem[P]]
      }
    }

    override def join[R](right: QueryBuilder[R])(on: (BPath, right.BPath) => Rep[Boolean]): QueryBuilder[(P, R)] = {
      // If we are pure, we can directly combine the fromItems
      (for
        leftPure  <- this.asPureFromItem
        rightPure <- right.asPureFromItem
      yield {
        makeSelect(
          FromItem.InnerJoin(leftPure, rightPure, on(leftPure.basePath, rightPure.basePath))
        )
      }).getOrElse {
        super.join(right)(on)
      }
    }

    override def leftJoin[R](
        right: QueryBuilder[R]
    )(on: (BPath, right.BPath) => Rep[Boolean]): QueryBuilder[(P, Option[R])] = {
      // If we are pure, we can directly combine the fromItems
      (for
        leftPure  <- this.asPureFromItem
        rightPure <- right.asPureFromItem
      yield {
        makeSelect(
          FromItem.LeftJoin(leftPure, rightPure, on(leftPure.basePath, rightPure.basePath))
        )
      }).getOrElse {
        super.leftJoin(right)(on)
      }
    }
  }

  private def makeSelect[T](from: FromItem[T]): Select[T, T] = Select(from, from.basePath)

  private def ensureFielded[T](in: SqlColumn[T] | SqlFielded[T]): SqlFielded[T] = {
    in match {
      case f: SqlFielded[T] => f
      case c: SqlColumn[T]  => SqlFielded.PseudoFielded(c)
    }
  }
}
