package usql.dao

import usql.{Optionalize, SqlColumnIdentifying, SqlInterpolationParameter, UnOption}

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
sealed trait ColumnPath[R, T] extends Selectable with SqlColumnIdentifying with Rep[T] {

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
  def structure: Structure[T]

  override final def toInterpolationParameter: SqlInterpolationParameter = columnIds

  override def toString: String = {
    columnIds match {
      case Seq(one) => one.toString
      case multiple => multiple.mkString("[", ",", "[")
    }
  }

  /** Prepend a path. */
  private[usql] def prepend[R2](columnPath: ColumnPath[R2, R]): ColumnPath[R2, T]
}

object ColumnPath {

  def make[T](using f: SqlFielded[T]): ColumnPath[T, T] = ColumnPathStart(f)

  case class Root[T](structure: Structure[T]) extends ColumnPath[T, T] {
    override def prepend[R2](path: ColumnPath[R2, T]): ColumnPath[R2, T] = {
      path
    }
  }

  case class Child[R, P, T](structure: Structure[T], parent: ColumnPath[R, P], field: String) extends ColumnPath[R, T] {
    override def prepend[R2](path: ColumnPath[R2, R]): ColumnPath[R2, T] = {
      parent.prepend(path).selectDynamic(field).asInstanceOf[ColumnPath[R2, T]]
    }
  }

  sealed trait TuplePath[R, T <: Tuple] extends ColumnPath[R, T] {
    override def structure: SqlFielded[T]

    override def prepend[R2](path: ColumnPath[R2, R]): TuplePath[R2, T]
  }

  case class EmptyTuplePath[R]() extends TuplePath[R, EmptyTuple] {
    override def prepend[R2](path: ColumnPath[R2, R]): TuplePath[R2, EmptyTuple] = EmptyTuplePath()

    override def structure: SqlFielded[EmptyTuple] = SqlFielded.emptyTuple
  }

  case class RecTuplePath[R, H, T <: Tuple](head: ColumnPath[R, H], tail: TuplePath[R, T])
    extends TuplePath[R, H *: T] {
    override def prepend[R2](path: ColumnPath[R2, R]): TuplePath[R2, H *: T] = RecTuplePath(
      head.prepend(path),
      tail.prepend(path)
    )

    override def structure: SqlFielded[H *: T] = SqlFielded.recursiveTuple(using head.structure, tail.structure)
  }

  /** Helper for building ColumnPath from Tuple */
  @implicitNotFound("Could not find BuildFromTuple")
  trait BuildFromTuple[T] {
    type CombinedType <: Tuple

    type Root

    def build(from: T): TuplePath[Root, CombinedType]
  }

  object BuildFromTuple {
    type Aux[T, C <: Tuple, R] = BuildFromTuple[T] {
      type CombinedType = C

      type Root = R
    }

    given empty[R]: BuildFromTuple.Aux[EmptyTuple, EmptyTuple, R] =
      new BuildFromTuple[EmptyTuple] {
        override type CombinedType = EmptyTuple

        override type Root = R

        override def build(from: EmptyTuple): TuplePath[R, EmptyTuple] = EmptyTuplePath[R]()
      }

    given rec[H, T <: Tuple, R, TC <: Tuple](
                                              using tailBuild: BuildFromTuple.Aux[T, TC, R]
                                            ): BuildFromTuple.Aux[
      (ColumnPath[R, H] *: T),
      H *: TC,
      R
    ] = new BuildFromTuple[ColumnPath[R, H] *: T] {
      override type CombinedType = H *: TC

      override type Root = R

      override def build(from: (ColumnPath[R, H] *: T)): TuplePath[R, CombinedType] = {
        RecTuplePath(from.head, tailBuild.build(from.tail))
      }
    }
  }

  /** Build a ColumnPath from a tuple of Column Paths. */
  implicit def fromTuple[T](in: T)(using b: BuildFromTuple[T]): ColumnPath[b.Root, b.CombinedType] =
    b.build(in)
}
