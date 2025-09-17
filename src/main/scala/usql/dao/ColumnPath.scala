package usql.dao

import usql.SqlInterpolationParameter.SqlParameter
import usql.{DataType, Sql, SqlIdentifier, SqlIdentifying, SqlInterpolationParameter, sql}

import scala.annotation.implicitNotFound
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

  implicit def fromTuple[T](in: T)(using b: BuildFromTuple[T]): ColumnPath[b.Root, b.CombinedType] =
    b.build(in)

  private def emptyPath[R]: ColumnPath[R, EmptyTuple] = ???

  private def prependPath[R, H, T <: Tuple](head: ColumnPath[R, H], tail: ColumnPath[R, T]): ColumnPath[R, H *: T] = ???

  @implicitNotFound("Could not find BuildFromTuple")
  trait BuildFromTuple[T] {
    type CombinedType <: Tuple

    type Root

    def build(from: T): ColumnPath[Root, CombinedType]
  }

  object BuildFromTuple {
    type Aux[T, C <: Tuple, R] = BuildFromTuple[T] {
      type CombinedType = C

      type Root = R
    }

    given buildFromEmptyTuple[R]: BuildFromTuple.Aux[EmptyTuple, EmptyTuple, R] =
      new BuildFromTuple[EmptyTuple] {
        override type CombinedType = EmptyTuple

        override type Root = R

        override def build(from: EmptyTuple): ColumnPath[R, EmptyTuple] = emptyPath[R]
      }

    given buildFromIteration[H, T <: Tuple, R, TC <: Tuple](using tailBuild: BuildFromTuple.Aux[T, TC, R]): BuildFromTuple.Aux[
      (ColumnPath[R, H] *: T),
      H *: TC,
      R
    ] = new BuildFromTuple[ColumnPath[R, H] *: T] {
      override type CombinedType = H *: TC

      override type Root = R

      override def build(from: (ColumnPath[R, H] *: T)): ColumnPath[R, CombinedType] = prependPath(from.head, tailBuild.build(from.tail))
    }
  }

}
