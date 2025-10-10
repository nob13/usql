package usql.dao

import usql.{Sql, SqlInterpolationParameter, sql}

import java.util.UUID

private[usql] trait QueryBuilderBase[T] extends QueryBuilder[T] {

  /** Returns the base path fore mapping operations. */
  protected def basePath: ColumnBasePath[T]

  /** Join two queries. */
  def join[R](right: QueryBuilder[R])(
      on: (ColumnBasePath[T], ColumnBasePath[R]) => Rep[Boolean]
  ): QueryBuilder[(T, R)] = {
    val leftSource  = this.asAliasedFromItem()
    val rightSource = right.asAliasedFromItem()
    val joinSource  = FromItem.InnerJoin(leftSource, rightSource, on(leftSource.basePath, rightSource.basePath))
    Select.makeSelect(joinSource)
  }

  /** Left Join two Queries */
  def leftJoin[R](right: QueryBuilder[R])(
      on: (ColumnBasePath[T], ColumnBasePath[R]) => Rep[Boolean]
  ): QueryBuilder[(T, Option[R])] = {
    val leftSource  = this.asAliasedFromItem()
    val rightSource = right.asAliasedFromItem()
    val joinSource  = FromItem.LeftJoin(leftSource, rightSource, on(leftSource.basePath, rightSource.basePath))
    Select.makeSelect(joinSource)
  }
}

private def ensureFielded[C](in: SqlColumn[C] | SqlFielded[C]): SqlFielded[C] = {
  in match {
    case f: SqlFielded[C] => f
    case c: SqlColumn[C]  => SqlFielded.PseudoFielded(c)
  }
}

/** A Generic select from a [[FromItem]] */
private[usql] class Select[T, P](from: FromItem[T], projection: ColumnPath[T, P], filters: Seq[Rep[Boolean]] = Nil)
    extends QueryBuilderBase[P] {
  override def toPreSql: Sql = {
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
    ensureFielded(projection.structure)
  }

  protected def basePath: ColumnPath[P, P] = {
    ColumnPath.make[P](using innerFielded)
  }

  override def filter(f: ColumnBasePath[P] => Rep[Boolean]): Select[T, P] = {
    Select(
      from,
      projection,
      filters = filters :+ f(basePath)
    )
  }

  override def project[P2](p: ColumnPath[P, P2]): Select[T, P2] = {
    Select(
      from,
      projection = ColumnPath.concat(projection, p),
      filters
    )
  }

  override def map[R0](f: ColumnPath[P, P] => ColumnPath[P, R0]): QueryBuilder[R0] = {
    project(f(basePath))
  }
}

private[usql] object Select {
  def makeSelect[T](from: FromItem[T]): Select[T, T] = Select(from, from.basePath)
}

private[usql] case class SimpleTableSelect[T](
    tabular: SqlTabular[T],
    filters: Seq[ColumnBasePath[T] => Rep[Boolean]] = Nil
) extends QueryBuilderForTable[T]
    with QueryBuilderBase[T] {

  override def filter(f: ColumnBasePath[T] => Rep[Boolean]): QueryBuilderForTable[T] = SimpleTableSelect(
    tabular,
    filters = filters :+ f
  )

  override def update(in: T): Long = ???

  override def project[P](p: ColumnPath[T, P]): QueryBuilderForProjectedTable[P] = {
    SimpleTableProject(this, p)
  }

  override def map[R0](f: ColumnPath[T, T] => ColumnPath[T, R0]): QueryBuilderForProjectedTable[R0] = {
    project(f(basePath))
  }

  override def toPreSql: Sql = {
    val maybeFilterSql: SqlInterpolationParameter = appliedFilters match {
      case Some(f) => sql"WHERE ${f.toInterpolationParameter}"
      case None    => SqlInterpolationParameter.Empty
    }
    sql"SELECT ${tabular.columns} FROM ${tabular.table} ${maybeFilterSql}"
  }

  def appliedFilters: Option[Rep[Boolean]] = {
    Option.when(filters.nonEmpty) {
      val bp = basePath
      filters.view
        .map { f =>
          f(bp)
        }
        .reduce(_ && _)
    }
  }

  override def fielded: SqlFielded[T] = tabular

  override def delete(): Long = ???

  protected def basePath: ColumnPath[T, T] = {
    ColumnPath.make[T](using tabular)
  }

  override private[usql] def asAliasedFromItem(): FromItem[T] = {
    if filters.isEmpty then {
      val aliasName = s"${tabular.table.name}-${UUID.randomUUID()}"
      FromItem.Aliased(FromItem.FromTable(tabular), aliasName)
    } else {
      super.asAliasedFromItem()
    }
  }
}

private[usql] case class SimpleTableProject[T, P](in: SimpleTableSelect[T], projection: ColumnPath[T, P])
    extends QueryBuilderForProjectedTable[P]
    with QueryBuilderBase[P] {
  override def update(in: P): Long = ???

  override def map[R0](f: ColumnPath[P, P] => ColumnPath[P, R0]): QueryBuilderForProjectedTable[R0] = {
    project(f(basePath))
  }

  override def project[X](p: ColumnPath[P, X]): QueryBuilderForProjectedTable[X] = {
    val newProjection = ColumnPath.concat(projection, p)
    copy(projection = newProjection)
  }

  override def toPreSql: Sql = {
    val maybeFilterSql: SqlInterpolationParameter = in.appliedFilters match {
      case Some(f) => sql"WHERE ${f.toInterpolationParameter}"
      case None    => SqlInterpolationParameter.Empty
    }

    val projectionString = SqlInterpolationParameter.MultipleSeparated(
      projection.columnIds.zip(fielded.columns.map(_.id)).map { case (p, as) =>
        sql"${p} AS ${as}"
      }
    )
    sql"SELECT ${projectionString} FROM ${in.tabular.table} ${maybeFilterSql}"
  }

  /** The fielded representation from the outside. */
  lazy val fielded: SqlFielded[P] = {
    innerFielded.ensureUniqueColumnIds(keepAlias = false)
  }

  /** The fielded representation inside (using aliases) */
  lazy val innerFielded: SqlFielded[P] = {
    ensureFielded(projection.structure)
  }

  override def filter(f: ColumnBasePath[P] => Rep[Boolean]): QueryBuilder[P] = {
    val mappedFilter: ColumnBasePath[T] => Rep[Boolean] = in => {
      f(ColumnPath.concat(in, projection))
    }
    copy(
      in.copy(
        filters = in.filters :+ mappedFilter
      )
    )
  }

  override protected def basePath: ColumnPath[P, P] = ColumnPath.make[P](using fielded)
}
