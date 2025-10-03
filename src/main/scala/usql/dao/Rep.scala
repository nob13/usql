package usql.dao

import usql.dao.Rep.SqlRep
import usql.{DataType, Sql, SqlInterpolationParameter, sql}

import scala.language.implicitConversions

/** Typed representation for a typed value (either a [[ColumnPath]] or some raw SQL) */
trait Rep[T] {
  def toInterpolationParameter: SqlInterpolationParameter

  def ===(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} = ${rep.toInterpolationParameter}")
  }

  def !==(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} <> ${rep.toInterpolationParameter}")
  }

  def <(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} < ${rep.toInterpolationParameter}")
  }

  def >(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} > ${rep.toInterpolationParameter}")
  }

  def <=(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} <= ${rep.toInterpolationParameter}")
  }

  def >=(rep: Rep[T]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} >= ${rep.toInterpolationParameter}")
  }

  def &&(using T =:= Boolean)(rep: Rep[Boolean]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} AND ${rep.toInterpolationParameter}")
  }

  def ||(using T =:= Boolean)(rep: Rep[Boolean]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} OR ${rep.toInterpolationParameter}")
  }

  def isNull(using T => Option[?]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} IS NULL")
  }

  def isNotNull(using T => Option[?]): Rep[Boolean] = {
    SqlRep(sql"${toInterpolationParameter} IS NOT NULL")
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

  case class SomeRep[T](underlying: Rep[T]) extends Rep[Option[T]] {
    override def toInterpolationParameter: SqlInterpolationParameter = underlying.toInterpolationParameter
  }

  implicit def raw[T: DataType](value: T): Rep[T]                         = RawValue(value)
  implicit def rawOpt[T](value: T)(using dt: DataType[T]): Rep[Option[T]] = opt(raw(value))
  implicit def opt[T](rep: Rep[T]): Rep[Option[T]]                        = SomeRep(rep)
}
