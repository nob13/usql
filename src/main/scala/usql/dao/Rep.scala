package usql.dao

import usql.dao.Rep.SqlRep
import usql.{DataType, Sql, SqlInterpolationParameter, sql}

import scala.language.implicitConversions

trait Rep[T] {
  def toInterpolationParameter: SqlInterpolationParameter

  def ===(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} = ${rep.toInterpolationParameter}")
  }

  def <=(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} <= ${rep.toInterpolationParameter}")
  }

  def >=(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} >= ${rep.toInterpolationParameter}")
  }
}

object Rep {
  case class SqlRep[T](rep: Sql) extends Rep[T] {
    override def toInterpolationParameter: SqlInterpolationParameter = rep
  }

  case class RawValue[T: DataType](value: T) extends Rep[T] {
    override def toInterpolationParameter: SqlInterpolationParameter =
      SqlInterpolationParameter.SqlParameter(value)
  }

  implicit def rawValue[T: DataType](value: T): Rep[T] = RawValue(value)
}
