package usql.dao

import usql.{ConnectionProvider, DataType, Query, RowDecoder, Sql, SqlIdentifier, SqlInterpolationParameter, sql}

object QueryBuilder {
  def from[T](using tabular: SqlTabular[T]): From[T] = From[T](tabular)

  case class From[T](tabular: SqlTabular[T], reverseWherePredicates: List[Rep[Boolean]] = Nil) {

    def where(pred: ColumnPath[T, T] => Rep[Boolean]): From[T] = {
      copy(
        reverseWherePredicates = pred(tabular.cols) :: reverseWherePredicates
      )
    }

    def selectAll: Selected[T, T] = Selected(this, tabular.columns.map(_.id).toList, tabular.rowDecoder)

    def select[A1](col1: ColumnPath[T, T] => ColumnPath[T, A1])(using rowDecoder: RowDecoder[A1]): Selected[T, A1] = {
      Selected[T, A1](
        this,
        List(col1(tabular.cols).buildIdentifier),
        rowDecoder
      )
    }

    def select[A1, A2](
        col1: ColumnPath[T, T] => ColumnPath[T, A1],
        col2: ColumnPath[T, T] => ColumnPath[T, A2]
    )(using rowDecoder: RowDecoder[(A1, A2)]): Selected[T, (A1, A2)] = {
      Selected(
        this,
        List(
          col1(tabular.cols).buildIdentifier,
          col2(tabular.cols).buildIdentifier
        ),
        rowDecoder
      )
    }

    def select[A1, A2, A3](
        col1: ColumnPath[T, T] => ColumnPath[T, A1],
        col2: ColumnPath[T, T] => ColumnPath[T, A2],
        col3: ColumnPath[T, T] => ColumnPath[T, A3]
    )(using rowDecoder: RowDecoder[(A1, A2, A3)]): Selected[T, (A1, A2, A3)] = {
      Selected[T, (A1, A2, A3)](
        this,
        List(
          col1(tabular.cols).buildIdentifier,
          col2(tabular.cols).buildIdentifier,
          col3(tabular.cols).buildIdentifier
        ),
        rowDecoder
      )
    }
  }

  case class Selected[T, R](from: From[T], selection: List[SqlIdentifier], decoder: RowDecoder[R]) {
    private def toQuery: Query = {
      val where: Sql = if from.reverseWherePredicates.isEmpty then {
        Sql(Nil)
      } else {
        sql"WHERE ${from.reverseWherePredicates.reverse.reduce(_ && _).toInterpolationParameter}"
      }

      Query(
        sql"SELECT ${selection} FROM ${from.tabular.tableName} ${where}"
      )
    }

    def one()(using cp: ConnectionProvider): Option[R] = {
      toQuery.one[R]()(using decoder)
    }

    def all()(using cp: ConnectionProvider): Vector[R] = {
      toQuery.all[R]()(using decoder)
    }
  }
}
