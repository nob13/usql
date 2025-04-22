package usql

import usql.dao.SqlColumnar
import java.sql.ResultSet

/** Decoder for singles rows in a [[ResultSet]] */
trait RowDecoder[T] {

  /** Parse a single row. */
  def parseRow(offset: Int, row: ResultSet): T

  /** Parse at offset 0 */
  def parseRow(row: ResultSet): T = parseRow(0, row)

  def map[U](f: T => U): RowDecoder[U] = {
    val me = this
    new RowDecoder[U] {
      override def parseRow(offset: Int, row: ResultSet): U = {
        f(me.parseRow(offset, row))
      }

      override def cardinality: Int = me.cardinality
    }
  }

  /** The number of elements consumed by the decoder. */
  def cardinality: Int
}

object RowDecoder {
  given forTuple[H, T <: Tuple](
      using headDecoder: RowDecoder[H],
      tailDecoder: RowDecoder[T]
  ): RowDecoder[H *: T] = new RowDecoder[H *: T] {
    override def parseRow(offset: Int, row: ResultSet): H *: T = {
      val h = headDecoder.parseRow(offset, row)
      val t = tailDecoder.parseRow(offset + headDecoder.cardinality, row)
      (h *: t)
    }

    override def cardinality: Int = headDecoder.cardinality + tailDecoder.cardinality
  }

  given empty: RowDecoder[EmptyTuple] = new RowDecoder[EmptyTuple] {
    override def parseRow(offset: Int, row: ResultSet): EmptyTuple = EmptyTuple

    override def cardinality: Int = 0
  }

  given forDataType[T](using dt: DataType[T]): RowDecoder[T] = new RowDecoder[T] {
    override def parseRow(offset: Int, row: ResultSet): T = dt.extractByZeroBasedIndex(offset, row)

    override def cardinality: Int = 1
  }

  given forOptional[T](using rd: RowDecoder[T]): RowDecoder[Option[T]] = new RowDecoder[Option[T]] {
    override def parseRow(offset: Int, row: ResultSet): Option[T] = {
      val isNone = (0 until cardinality).forall { baseIdx =>
        val cIdx = offset + baseIdx + 1
        val _    = row.getObject(cIdx)
        row.wasNull()
      }
      if isNone then {
        None
      } else {
        val inner = rd.parseRow(offset, row)
        Some(inner)
      }
    }

    override def cardinality: Int = rd.cardinality
  }

  given forColumnar[T](using c: SqlColumnar[T]): RowDecoder[T] = c.rowDecoder
}
