package usql.dao

import usql.SqlInterpolationParameter.SqlParameter
import usql.{DataType, Sql, SqlIdentifier, SqlIdentifying, SqlInterpolationParameter, sql}

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
trait ColumnPath[R, T] extends Selectable with SqlIdentifying with Rep[T] {
  final type Child[X] = ColumnPath[R, X]

  /** Names the Fields of this ColumnPath. */
  type Fields = NamedTuple.Map[NamedTuple.From[T], Child]

  /** Select a dynamic field. */
  def selectDynamic(name: String): ColumnPath[R, ?]

  /** Unpack an option. */
  def ![X](using ev: T => Option[X]): ColumnPath[R, X]

  /** Build a getter for this field from the base type. */
  def buildGetter: R => T
}

object ColumnPath {

  def make[T](using f: SqlFielded[T]): ColumnPath[T, T] = ColumnPathImpl(f)
}
