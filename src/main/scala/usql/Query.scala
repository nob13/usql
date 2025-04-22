package usql

import java.sql.{Connection, ResultSet}
import scala.util.Using

/** The user wants o issue a query. */
case class Query(sql: SqlBase) {

  /** Run a query for one row. */
  def one[T]()(using rowParser: RowDecoder[T], cp: ConnectionProvider): Option[T] = {
    run { resultSet =>
      if resultSet.next() then {
        Some(rowParser.parseRow(resultSet))
      } else {
        None
      }
    }
  }

  /** Run a query for all rows. */
  def all[T]()(using rowParser: RowDecoder[T], cp: ConnectionProvider): Vector[T] = {
    run { resultSet =>
      val builder = Vector.newBuilder[T]
      while resultSet.next() do {
        builder += rowParser.parseRow(resultSet)
      }
      builder.result()
    }
  }

  /** Run with some method decoding the result set. */
  private def run[T](f: ResultSet => T)(using cp: ConnectionProvider): T = {
    sql.withPreparedStatement { statement =>
      Using.resource(statement.executeQuery()) { resultSet =>
        f(resultSet)
      }
    }
  }
}
