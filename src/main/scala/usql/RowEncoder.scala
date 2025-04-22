package usql

import usql.dao.SqlColumnar

import java.sql.PreparedStatement

/** Responsible for filling arguments into prepared statements for batch operations. */
trait RowEncoder[T] {

  /** Fill something at the zero-based position index into the prepared statement. */
  def fill(offset: Int, ps: PreparedStatement, value: T): Unit

  /** Fill some value without type checking. */
  private[usql] def fillUnchecked(offset: Int, ps: PreparedStatement, value: Any): Unit = {
    fill(offset, ps, value.asInstanceOf[T])
  }

  /** Fill at position 0 */
  def fill(ps: PreparedStatement, value: T): Unit = fill(0, ps, value)

  def contraMap[U](f: U => T): RowEncoder[U] = {
    val me = this
    new RowEncoder[U] {
      override def fill(offset: Int, ps: PreparedStatement, value: U): Unit = me.fill(offset, ps, f(value))

      override def cardinality: Int = me.cardinality
    }
  }

  /** The number of elements set by this filler */
  def cardinality: Int
}

object RowEncoder {

  given forTuple[H, T <: Tuple](
      using headFiller: RowEncoder[H],
      tailFiller: RowEncoder[T]
  ): RowEncoder[H *: T] = new RowEncoder[H *: T] {
    override def fill(offset: Int, ps: PreparedStatement, value: H *: T): Unit = {
      headFiller.fill(offset, ps, value.head)
      tailFiller.fill(offset + headFiller.cardinality, ps, value.tail)
    }

    override def cardinality: Int = {
      headFiller.cardinality + tailFiller.cardinality
    }
  }

  given empty: RowEncoder[EmptyTuple] = new RowEncoder[EmptyTuple] {
    override def fill(offset: Int, ps: PreparedStatement, value: EmptyTuple): Unit = ()

    override def cardinality: Int = 0
  }

  given forDataType[T](using dt: DataType[T]): RowEncoder[T] = new RowEncoder[T] {
    override def fill(offset: Int, ps: PreparedStatement, value: T): Unit = dt.fillByZeroBasedIdx(offset, ps, value)

    override def cardinality: Int = 1
  }

  given forColumnar[T](using c: SqlColumnar[T]): RowEncoder[T] = c.rowEncoder
}
