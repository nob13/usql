package usql.dao

import usql.{RowDecoder, Sql, SqlIdentifier, SqlIdentifying}

object QueryBuilder {
  def from[T](using tabular: SqlTabular[T]): From[T] = From[T](tabular)

  case class From[T](tabular: SqlTabular[T], where: List[Sql] = Nil) {
    def where(sql: Sql): From[T] = copy(where = sql :: where)

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

  case class Selected[T, R](from: From[T], selection: List[SqlIdentifier], decoder: RowDecoder[R])

  // TODO:
  // - Nice Select of fields, so that types are automatically detected.
  // - Nice defining of where clauses
  // - Can we automate the select calls?
  // - Starting Point should be the SqlFielded and or CrdRepo
  // Not-TODO (yet)
  // - Joins
}
