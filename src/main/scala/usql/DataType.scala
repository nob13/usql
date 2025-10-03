package usql

import java.sql.{Connection, JDBCType, PreparedStatement, ResultSet}

/** Type class describing a type to use. */
trait DataType[T] {

  /** Name of the data type. */
  def name: String

  /** Serialize a value (e.g. Debugging) */
  def serialize(value: T): String = value.toString

  /** The underlying jdbc type. */
  def jdbcType: JDBCType

  // Extractors from ResultSet

  def extractByZeroBasedIndex(idx: Int, rs: ResultSet): T = {
    extractBySqlIdx(idx + 1, rs)
  }

  def extractBySqlIdx(cIdx: Int, rs: ResultSet): T

  def extractOptionalBySqlIdx(cIdx: Int, rs: ResultSet): Option[T] = {
    val candidate = Option(extractBySqlIdx(cIdx, rs))
    if rs.wasNull() then {
      None
    } else {
      candidate
    }
  }

  def extractByName(columnLabel: String, resultSet: ResultSet): T = {
    val sqlIdx = resultSet.findColumn(columnLabel)
    extractBySqlIdx(sqlIdx, resultSet)
  }

  // Fillers

  def fillBySqlIdx(pIdx: Int, ps: PreparedStatement, value: T): Unit

  def fillByZeroBasedIdx(idx: Int, ps: PreparedStatement, value: T): Unit = {
    fillBySqlIdx(idx + 1, ps, value)
  }

  override def toString: String = name

  /** Adapt to another type. */
  def adapt[U](newName: String, mapFn: T => U, contraMapFn: U => T): DataType[U] = {
    val me = this
    new DataType[U] {
      override def jdbcType: JDBCType = me.jdbcType

      override def name: String = newName

      override def extractBySqlIdx(cIdx: Int, rs: ResultSet): U = mapFn(me.extractBySqlIdx(cIdx, rs))

      override def extractOptionalBySqlIdx(cIdx: Int, rs: ResultSet): Option[U] = {
        me.extractOptionalBySqlIdx(cIdx, rs).map(mapFn)
      }

      override def fillBySqlIdx(pIdx: Int, ps: PreparedStatement, value: U): Unit = {
        me.fillBySqlIdx(pIdx, ps, contraMapFn(value))
      }
    }
  }

  /** Adapt to another type, also providing the prepared statement */
  def adaptWithPs[U <: AnyRef](
      newName: String,
      mapFn: T => U,
      contraMapFn: (U, PreparedStatement) => T
  ): DataType[U] = {
    val me = this
    new DataType[U] {
      override def jdbcType: JDBCType = me.jdbcType

      override def name: String = newName

      override def extractBySqlIdx(cIdx: Int, rs: ResultSet): U = {
        val inner = me.extractBySqlIdx(cIdx, rs)
        if inner == null then {
          null.asInstanceOf[U]
        } else {
          mapFn(me.extractBySqlIdx(cIdx, rs))
        }
      }

      override def fillBySqlIdx(pIdx: Int, ps: PreparedStatement, value: U): Unit =
        me.fillBySqlIdx(pIdx, ps, contraMapFn(value, ps))
    }
  }

  /** Returns the optional variant of this data type. */
  def optionalize: DataType[Optionalize[T]] = DataType.OptionalDataType(this).asInstanceOf[DataType[Optionalize[T]]]

  /** Returns true if this is an optional value. */
  def isOptional: Boolean = false
}

object DataType {
  def simple[T](
      newName: String,
      jdbc: JDBCType,
      rsExtractor: (ResultSet, Int) => T,
      filler: (PreparedStatement, Int, T) => Unit
  ): DataType[T] = new DataType[T] {
    override def name: String = newName

    override def jdbcType: JDBCType = jdbc

    override def extractBySqlIdx(cIdx: Int, rs: ResultSet): T = rsExtractor(rs, cIdx)

    override def fillBySqlIdx(pIdx: Int, ps: PreparedStatement, value: T): Unit = filler(ps, pIdx, value)
  }

  def get[T](using dt: DataType[T]): DataType[T] = dt

  case class OptionalDataType[T](underlying: DataType[T]) extends DataType[Option[T]] {
    override def name: String = s"Option[${underlying.name}]"

    override def extractBySqlIdx(cIdx: Int, rs: ResultSet): Option[T] = {
      underlying.extractOptionalBySqlIdx(cIdx, rs)
    }

    override def fillBySqlIdx(pIdx: Int, ps: PreparedStatement, value: Option[T]): Unit = {
      value match {
        case None    => ps.setNull(pIdx, jdbcType.getVendorTypeNumber)
        case Some(v) => underlying.fillBySqlIdx(pIdx, ps, v)
      }
    }

    override def jdbcType: JDBCType = underlying.jdbcType

    override def optionalize: DataType[Option[T]] = this

    override def isOptional: Boolean = true
  }

  given optionType[T](using dt: DataType[T]): DataType[Option[T]] = new OptionalDataType(dt)
}
