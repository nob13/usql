package usql

import java.sql.{Connection, ResultSet}
import scala.util.Using

/** An SQL Query */
trait Query[T] {

  /** The SQL of the Query */
  def sql: SqlBase

  /** The row decoder */
  def rowDecoder: RowDecoder[T]

  /** Run a query for one row. */
  def one()(using cp: ConnectionProvider): Option[T] = {
    run { resultSet =>
      if resultSet.next() then {
        Some(rowDecoder.parseRow(resultSet))
      } else {
        None
      }
    }
  }

  /** Run a query for all rows. */
  def all()(using cp: ConnectionProvider): Vector[T] = {
    run { resultSet =>
      val builder = Vector.newBuilder[T]
      while resultSet.next() do {
        builder += rowDecoder.parseRow(resultSet)
      }
      builder.result()
    }
  }

  /** Run with some method decoding the result set. */
  private def run[X](f: ResultSet => X)(using cp: ConnectionProvider): X = {
    sql.withPreparedStatement { statement =>
      Using.resource(statement.executeQuery()) { resultSet =>
        f(resultSet)
      }
    }
  }
}

object Query {
  case class Simple[T](sql: SqlBase, rowDecoder: RowDecoder[T]) extends Query[T]
}
