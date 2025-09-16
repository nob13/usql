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

  implicit def fromTuple[R, T <: Tuple, C <: Tuple](using b: BuildFromTuple.Aux[R, T, C])(in: T): ColumnPath[R, C] =
    b.build(in)

  private def emptyPath[R]: ColumnPath[R, EmptyTuple] = ???

  private def prependPath[R, H, T <: Tuple](head: ColumnPath[R, H], tail: ColumnPath[R, T]): ColumnPath[R, H *: T] = ???

  trait BuildFromTuple[R, T <: Tuple] {
    type CombinedType <: Tuple

    final type Result = ColumnPath[R, CombinedType]

    def build(from: T): Result
  }

  object BuildFromTuple {
    type Aux[R, T <: Tuple, C <: Tuple] = BuildFromTuple[R, T] {
      type CombinedType = C
    }
  }

  given buildFromEmptyTuple[R]: BuildFromTuple.Aux[R, EmptyTuple, EmptyTuple] =
    new BuildFromTuple[R, EmptyTuple] {
      override type CombinedType = EmptyTuple

      override def build(from: EmptyTuple): Result = emptyPath[R]
    }

  given buildFromIteration[R, H, T <: Tuple](using tailBuild: BuildFromTuple[R, T]): BuildFromTuple.Aux[
    R,
    (ColumnPath[R, H] *: T),
    H *: tailBuild.CombinedType
  ] = new BuildFromTuple[R, ColumnPath[R, H] *: T] {
    override type CombinedType = H *: tailBuild.CombinedType

    override def build(from: (ColumnPath[R, H] *: T)): Result = prependPath(from.head, tailBuild.build(from.tail))
  }

}
