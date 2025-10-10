package usql.dao

import usql.{Sql, SqlInterpolationParameter, sql}

/** Defines a DataSource within [[QueryBuilder]] */
private[usql] sealed trait FromItem[T] {
  def fielded: SqlFielded[T]

  def toPreSql: Sql

  lazy val basePath: ColumnPath[T, T] = ColumnPath.make(using fielded)
}

private[usql] object FromItem {
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
