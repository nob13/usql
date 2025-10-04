package usql

import usql.dao.{Alias, CrdBase, SqlColumn}

import scala.language.implicitConversions

/** Parameters available in sql""-Interpolation. */
sealed trait SqlInterpolationParameter {

  /** Converts to SQL or to replacement character. */
  def toSql: String

  /** Collect all used aliases. */
  def collectAliases: Set[String] = Set.empty

  /** Renames aliases. */
  def mapAliases(map: Map[String, String]): SqlInterpolationParameter = this
}

object SqlInterpolationParameter {

  /** A Parameter which will be filled using '?' and parameter filler */
  class SqlParameter[T](val value: T, val dataType: DataType[T]) extends SqlInterpolationParameter {
    override def equals(obj: Any): Boolean = {
      obj match {
        case s: SqlParameter[_] if value == s.value && dataType == s.dataType => true
        case _                                                                => false
      }
    }

    override def hashCode(): Int = {
      value.hashCode()
    }

    override def toSql: String = "?"

    override def toString: String = {
      s"SqlParameter(${value} of type ${dataType.jdbcType.getName})"
    }
  }

  object SqlParameter {
    def apply[T](value: T)(using dataType: DataType[T]): SqlParameter[T] = new SqlParameter(value, dataType)
  }

  /** A single identifier. */
  case class ColumnIdParameter(i: SqlColumnId) extends SqlInterpolationParameter {
    override def toSql: String = i.serialize

    override def collectAliases: Set[String] = i.alias.toSet

    override def mapAliases(map: Map[String, String]): SqlInterpolationParameter = copy(
      i.copy(
        alias = i.alias.map { alias =>
          map.getOrElse(alias, alias)
        }
      )
    )
  }

  /** Multiple identifiers. */
  case class IdentifiersParameter(i: Seq[SqlColumnId]) extends SqlInterpolationParameter {
    override def toSql: String = {
      i.iterator.map(_.serialize).mkString(",")
    }

    override def collectAliases: Set[String] = i.flatMap(_.alias).toSet

    override def mapAliases(map: Map[String, String]): SqlInterpolationParameter = {
      i.map { id =>
        id.copy(
          alias = id.alias.map { alias =>
            map.getOrElse(alias, alias)
          }
        )
      }
    }
  }

  /** Some unchecked raw block. */
  case class RawBlockParameter(s: String) extends SqlInterpolationParameter {
    override def toSql: String = s
  }

  case class InnerSql(sql: Sql) extends SqlInterpolationParameter {
    // Not used
    override def toSql: String = sql.sql

    override def collectAliases: Set[String] = sql.collectAliases

    override def mapAliases(map: Map[String, String]): SqlInterpolationParameter = sql.mapAliases(map)
  }

  case class TableRef(tableId: SqlTableId, alias: Option[String] = None) extends SqlInterpolationParameter {
    override def toSql: String = {
      val builder = StringBuilder()
      builder ++= tableId.serialize
      alias.foreach { alias =>
        builder += ' '
        builder ++= alias
      }
      builder.toString()
    }

    override def collectAliases: Set[String] = alias.toSet

    override def mapAliases(map: Map[String, String]): SqlInterpolationParameter = {
      copy(
        alias = alias.map { a => map.getOrElse(a, a) }
      )
    }
  }

  /** Empty leaf, so that we have exactly as much interpolation parameters as string parts. */
  object Empty extends SqlInterpolationParameter {
    override def toSql: String = ""
  }

  implicit def toSqlParameter[T](value: T)(using dataType: DataType[T]): SqlParameter[T] = {
    new SqlParameter(value, dataType)
  }

  implicit def toIdentifierParameter(i: SqlColumnIdentifying): IdentifiersParameter       = IdentifiersParameter(
    i.columnIds
  )
  implicit def toIdentifiersParameter(i: Seq[SqlColumnIdentifying]): IdentifiersParameter = IdentifiersParameter(
    i.flatMap(_.columnIds)
  )
  implicit def columnsParameter(c: Seq[SqlColumn[?]]): IdentifiersParameter               = IdentifiersParameter(c.map(_.id))
  implicit def rawBlockParameter(rawPart: SqlRawPart): RawBlockParameter                  = {
    RawBlockParameter(rawPart.s)
  }
  implicit def innerSql(sql: Sql): InnerSql                                               = InnerSql(sql)
  implicit def alias(alias: Alias[?]): TableRef                                           = TableRef(
    tableId = alias.tabular.table,
    alias = Some(alias.aliasName)
  )
  implicit def tableId(tableId: SqlTableId): TableRef                                     = TableRef(tableId, None)

  implicit def sqlParameters[T](sqlParameters: SqlParameters[T])(using dataType: DataType[T]): InnerSql = {
    val builder = Seq.newBuilder[(String, SqlInterpolationParameter)]
    sqlParameters.values.headOption.foreach { first =>
      builder += (("", SqlParameter(first)))
      sqlParameters.values.tail.foreach { next =>
        builder += ((",", SqlParameter(next)))
      }
    }
    InnerSql(Sql(builder.result()))
  }

  implicit def crd(crd: CrdBase[?]): TableRef = TableRef(
    crd.tabular.table,
    alias = None
  )
}

/** Something which can be added to sql""-interpolation without further checking. */
case class SqlRawPart(s: String) {
  override def toString: String = s
}

/** Marker for a sequence of elements like in SQL IN Clause, will be encoded as `?,...,?` and filled with values */
case class SqlParameters[T](values: Seq[T])
