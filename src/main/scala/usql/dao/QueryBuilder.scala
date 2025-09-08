package usql.dao

import usql.{ConnectionProvider, DataType, Query, RowDecoder, Sql, SqlIdentifier, SqlInterpolationParameter, sql}

object QueryBuilder {
  def from[T](using tabular: SqlTabular[T]): Select[T] = Select[T](From.Simple(tabular))

  /** Encodes the source of data. */
  sealed trait From[T] {
    def fielded: SqlFielded[T]

    def encode: Sql
  }

  object From {
    case class Simple[T](tabular: SqlTabular[T]) extends From[T] {
      override def fielded: SqlFielded[T] = tabular

      override def encode: Sql = sql"${tabular.tableName}"
    }
  }

  /** Encodes a Select. */
  case class Select[T](source: From[T], reverseWherePredicates: List[Rep[Boolean]] = Nil) {

    def where(pred: ColumnPath[T, T] => Rep[Boolean]): Select[T] = {
      copy(
        reverseWherePredicates = pred(path) :: reverseWherePredicates
      )
    }

    private def path: ColumnPath[T, T] = source.fielded.cols

    def selectAll: Selected[T, T] = Selected(this, source.fielded.columns.map(_.id).toList, source.fielded.rowDecoder)

    def select[A1](col1: ColumnPath[T, T] => ColumnPath[T, A1])(using rowDecoder: RowDecoder[A1]): Selected[T, A1] = {
      Selected[T, A1](
        this,
        List(col1(path).buildIdentifier),
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
          col1(path).buildIdentifier,
          col2(path).buildIdentifier
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
          col1(path).buildIdentifier,
          col2(path).buildIdentifier,
          col3(path).buildIdentifier
        ),
        rowDecoder
      )
    }
  }

  case class Selected[T, R](from: Select[T], selection: List[SqlIdentifier], decoder: RowDecoder[R]) {
    private def toQuery: Query = {
      val where: Sql = if from.reverseWherePredicates.isEmpty then {
        Sql(Nil)
      } else {
        sql"WHERE ${from.reverseWherePredicates.reverse.reduce(_ && _).toInterpolationParameter}"
      }

      Query(
        sql"SELECT ${selection} FROM ${from.source.encode} ${where}"
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
