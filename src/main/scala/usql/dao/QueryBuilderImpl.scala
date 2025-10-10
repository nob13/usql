package usql.dao

import usql.{ConnectionProvider, Sql, SqlInterpolationParameter, sql}

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

/** Something is projected. */
private[usql] trait QueryBuilderProjected[T, P] extends QueryBuilder[P] {

  /** The projection. */
  def projection: ColumnPath[T, P]

  /** The fielded representation from the outside. */
  lazy val fielded: SqlFielded[P] = {
    innerFielded.ensureUniqueColumnIds(keepAlias = false)
  }

  /** The fielded representation inside. */
  lazy val innerFielded: SqlFielded[P] = {
    ensureFielded(projection.structure)
  }

  /** The projection SQL String */
  protected def projectionString = SqlInterpolationParameter.MultipleSeparated(
    projection.columnIds.zip(fielded.columns.map(_.id)).map { case (p, as) =>
      sql"${p} AS ${as}"
    }
  )
}

private def ensureFielded[C](in: SqlColumn[C] | SqlFielded[C]): SqlFielded[C] = {
  in match {
    case f: SqlFielded[C] => f
    case c: SqlColumn[C]  => SqlFielded.PseudoFielded(c)
  }
}

/** A Generic select from a [[FromItem]] */
private[usql] case class Select[T, P](from: FromItem[T], projection: ColumnPath[T, P], filters: Seq[Rep[Boolean]] = Nil)
    extends QueryBuilderBase[P]
    with QueryBuilderProjected[T, P] {
  override def toPreSql: Sql = {
    val maybeFilterSql: SqlInterpolationParameter = if filters.isEmpty then {
      SqlInterpolationParameter.Empty
    } else {
      sql" WHERE ${filters.reduce(_ && _).toInterpolationParameter}"
    }
    sql"SELECT ${projectionString} FROM ${from.toPreSql} ${maybeFilterSql}"
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

  override def project[P](p: ColumnPath[T, P]): QueryBuilderForProjectedTable[P] = {
    SimpleTableProject(this, p)
  }

  override def map[R0](f: ColumnPath[T, T] => ColumnPath[T, R0]): QueryBuilderForProjectedTable[R0] = {
    project(f(basePath))
  }

  override def toPreSql: Sql = {
    sql"SELECT ${tabular.columns} FROM ${tabular.table} ${maybeFilterSql}"
  }

  def maybeFilterSql: SqlInterpolationParameter = {
    appliedFilters match {
      case Some(f) => sql"WHERE ${f.toInterpolationParameter}"
      case None    => SqlInterpolationParameter.Empty
    }
  }

  override def update(in: T)(using cp: ConnectionProvider): Int = ???

  override def delete()(using cp: ConnectionProvider): Int = {
    sql"DELETE FROM ${tabular.table} ${maybeFilterSql}".update.run()
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
    with QueryBuilderBase[P]
    with QueryBuilderProjected[T, P] {
  override def update(value: P)(using cp: ConnectionProvider): Int = {
    val split  = fielded.rowEncoder.toSqlParameter(value)
    val setter = SqlInterpolationParameter.MultipleSeparated(
      fielded.columns.zip(split).map { case (column, value) =>
        sql"${column.id} = ${value}"
      }
    )
    val sql    = sql"UPDATE ${in.tabular.table} SET $setter ${in.maybeFilterSql}"
    sql.update.run()
  }

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

    sql"SELECT ${projectionString} FROM ${in.tabular.table} ${in.maybeFilterSql}"
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
