package usql.dao

import usql.dao.TupleColumnPath.BuildFromTuple
import usql.{Optionalize, SqlColumnIdentifying, SqlInterpolationParameter, UnOption}

import scala.language.implicitConversions

/**
 * Helper for going through the field path of SqlFielded.
 *
 * They can provide Identifiers and build getters like lenses do.
 *
 * @tparam R
 *   root model
 * @tparam T
 *   end path
 */
trait ColumnPath[R, T] extends Selectable with SqlColumnIdentifying with Rep[T] {

  /**
   * If we are coming from an optional value, we go into an optional value.
   *
   * If not, we take the child value.
   */
  final type Child[X] = T match {
    case Option[r] => ColumnPath[R, Optionalize[X]]
    case _         => ColumnPath[R, X]
  }

  /** Names the Fields of this ColumnPath. */
  type Fields = NamedTuple.Map[NamedTuple.From[UnOption[T]], Child]

  /** Select a dynamic field. */
  def selectDynamic(name: String): ColumnPath[R, ?]

  /** Build a getter for this field from the base type. */
  def buildGetter: R => T

  /** The structure of T */
  def structure: SqlFielded[T] | SqlColumn[T]

  override final def toInterpolationParameter: SqlInterpolationParameter = columnIds

  override def toString: String = {
    columnIds match {
      case Seq(one) => one.toString
      case multiple => multiple.mkString("[", ",", "[")
    }
  }

  /** Prepend a path. */
  private[usql] def prepend[R2](columnPath: ColumnPath[R2, R]): ColumnPath[R2, T]

  /** Returns true if this is an empty path */
  def isEmpty: Boolean = false
}

object ColumnPath {

  def make[T](using f: SqlFielded[T]): ColumnPath[T, T] = ColumnPathStart(f)

  def makeOpt[T](using f: SqlFielded[T]): ColumnPath[Option[T], Option[T]] = {
    ColumnPathStart(SqlFielded.OptionalSqlFielded(f))
  }

  /** Concat two Column Paths. */
  def concat[A, B, C](first: ColumnPath[A, B], second: ColumnPath[B, C]): ColumnPath[A, C] = {
    second.prepend(first)
  }

  /** Build a ColumnPath from a tuple of Column Paths. */
  implicit def fromTuple[T](in: T)(using b: BuildFromTuple[T]): ColumnPath[b.Root, b.CombinedType] =
    b.build(in)
}
