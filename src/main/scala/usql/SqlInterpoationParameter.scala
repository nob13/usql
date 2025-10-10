package usql

import usql.SqlInterpolationParameter.SqlParameter
import usql.dao.{Alias, CrdBase, SqlColumn}

import scala.language.implicitConversions

/** Parameters available in sql""-Interpolation. */
sealed trait SqlInterpolationParameter {

  /** Converts to SQL or to replacement character. */
  final def toSql: String = {
    val s = StringBuilder()
    serializeSql(s)
    s.toString()
  }

  def serializeSql(s: StringBuilder): Unit

  /** Collect all used aliases. */
  def collectAliases: Set[String] = Set.empty

  /** Renames aliases. */
  def mapAliases(map: Map[String, String]): SqlInterpolationParameter = this

  /** Collect nested parameters. */
  def parameters: Seq[SqlParameter[?]] = Nil
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

    override def serializeSql(s: StringBuilder): Unit = s += '?'

    override def toString: String = {
      s"SqlParameter(${value} of type ${dataType.jdbcType.getName})"
    }

    override def parameters: Seq[SqlParameter[_]] = List(this)
  }

  object SqlParameter {
    def apply[T](value: T)(using dataType: DataType[T]): SqlParameter[T] = new SqlParameter(value, dataType)
  }

  /** A single identifier. */
  case class ColumnIdParameter(columnId: SqlColumnId) extends SqlInterpolationParameter {
    override def serializeSql(s: StringBuilder): Unit = columnId.serializeSql(s)

    override def collectAliases: Set[String] = columnId.alias.toSet

    override def mapAliases(map: Map[String, String]): SqlInterpolationParameter = copy(
      columnId.copy(
        alias = columnId.alias.map { alias =>
          map.getOrElse(alias, alias)
        }
      )
    )
  }

  case class MultipleSeparated(underlying: Seq[SqlInterpolationParameter], separator: String = ",")
      extends SqlInterpolationParameter {
    override def serializeSql(s: StringBuilder): Unit = {
      if underlying.isEmpty then {
        return
      }
      underlying.head.serializeSql(s)
      underlying.tail.foreach { x =>
        s ++= separator
        x.serializeSql(s)
      }
    }

    override def collectAliases: Set[String] = underlying.toSet.flatMap(_.collectAliases)

    override def mapAliases(map: Map[String, String]): SqlInterpolationParameter = {
      copy(
        underlying = underlying.map(_.mapAliases(map))
      )
    }

    override def parameters: Seq[SqlParameter[_]] = underlying.flatMap(_.parameters)
  }

  /** Some unchecked raw block. */
  case class RawBlockParameter(param: String) extends SqlInterpolationParameter {
    override def serializeSql(s: StringBuilder): Unit = {
      s ++= param
    }
  }

  case class InnerSql(sql: Sql) extends SqlInterpolationParameter {
    override def serializeSql(s: StringBuilder): Unit = {
      sql.serializeSql(s)
    }

    override def collectAliases: Set[String] = sql.collectAliases

    override def mapAliases(map: Map[String, String]): SqlInterpolationParameter = sql.mapAliases(map)

    override def parameters: Seq[SqlParameter[_]] = sql.parameters
  }

  case class TableIdParameter(tableId: SqlTableId) extends SqlInterpolationParameter {
    override def serializeSql(s: StringBuilder): Unit = {
      tableId.serializeSql(s)
    }
  }

  case class AliasParameter(alias: String) extends SqlInterpolationParameter {
    override def serializeSql(s: StringBuilder): Unit = {
      s ++= alias
    }

    override def collectAliases: Set[String] = Set(alias)

    override def mapAliases(map: Map[String, String]): AliasParameter = AliasParameter(
      alias = map.getOrElse(alias, alias)
    )
  }

  /** Empty leaf, so that we have exactly as much interpolation parameters as string parts. */
  object Empty extends SqlInterpolationParameter {
    override def serializeSql(s: StringBuilder): Unit = {}
  }

  implicit def toSqlParameter[T](value: T)(using dataType: DataType[T]): SqlParameter[T] = {
    new SqlParameter(value, dataType)
  }

  implicit def toIdentifierParameter(i: SqlColumnIdentifying): MultipleSeparated       = MultipleSeparated(
    i.columnIds.map(ColumnIdParameter.apply)
  )
  implicit def toIdentifiersParameter(i: Seq[SqlColumnIdentifying]): MultipleSeparated = MultipleSeparated(
    i.flatMap(_.columnIds).map(ColumnIdParameter.apply)
  )
  implicit def columnsParameter(c: Seq[SqlColumn[?]]): MultipleSeparated               = toIdentifiersParameter(c.map(_.id))
  implicit def rawBlockParameter(rawPart: SqlRawPart): RawBlockParameter               = {
    RawBlockParameter(rawPart.s)
  }
  implicit def innerSql(sql: Sql): InnerSql                                            = InnerSql(sql)
  implicit def alias(alias: Alias[?]): InnerSql                                        = InnerSql(
    sql"${alias.tabular.table} AS ${AliasParameter(alias.aliasName)}"
  )

  implicit def tableId(tableId: SqlTableId): TableIdParameter = TableIdParameter(tableId)

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

  implicit def crd(crd: CrdBase[?]): TableIdParameter = TableIdParameter(
    crd.tabular.table
  )
}

/** Something which can be added to sql""-interpolation without further checking. */
case class SqlRawPart(s: String) {
  override def toString: String = s
}

/** Marker for a sequence of elements like in SQL IN Clause, will be encoded as `?,...,?` and filled with values */
case class SqlParameters[T](values: Seq[T])
