package usql

import usql.SqlInterpolationParameter.SqlParameter
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

      override def serialize(value: U): Seq[Any] = me.serialize(f(value))

      override def toSqlParameter(value: U): Seq[SqlParameter[_]] = me.toSqlParameter(f(value))
    }
  }

  /** The number of elements set by this filler */
  def cardinality: Int

  /** Serialize into values matching the DataTypes of [[SqlColumnar]] */
  def serialize(value: T): Seq[Any]

  /** Serialize something without type checking. */
  private[usql] def serializeUnchecked(value: Any): Seq[Any] = serialize(value.asInstanceOf[T])

  /** Convert a value into SQL Parameters. */
  def toSqlParameter(value: T): Seq[SqlParameter[?]]

  private[usql] def toSqlParameterUnchecked(value: Any): Seq[SqlParameter[?]] = toSqlParameter(value.asInstanceOf[T])
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

    override def serialize(value: H *: T): Seq[Any] = {
      headFiller.serialize(value.head) ++ tailFiller.serialize(value.tail)
    }

    override def toSqlParameter(value: H *: T): Seq[SqlParameter[_]] = {
      headFiller.toSqlParameter(value.head) ++ tailFiller.toSqlParameter(value.tail)
    }
  }

  given empty: RowEncoder[EmptyTuple] = new RowEncoder[EmptyTuple] {
    override def fill(offset: Int, ps: PreparedStatement, value: EmptyTuple): Unit = ()

    override def cardinality: Int = 0

    override def serialize(value: EmptyTuple): Seq[Any] = Nil

    override def toSqlParameter(value: EmptyTuple): Seq[SqlParameter[_]] = Nil
  }

  given forDataType[T](using dt: DataType[T]): RowEncoder[T] = new RowEncoder[T] {
    override def fill(offset: Int, ps: PreparedStatement, value: T): Unit = dt.fillByZeroBasedIdx(offset, ps, value)

    override def cardinality: Int = 1

    override def serialize(value: T): Seq[Any] = Seq(value)

    override def toSqlParameter(value: T): Seq[SqlParameter[_]] = {
      List(SqlParameter(value))
    }
  }

  given forColumnar[T](using c: SqlColumnar[T]): RowEncoder[T] = c.rowEncoder
}
