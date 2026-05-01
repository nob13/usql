package usql.dao

import usql.{Optionalize, SqlColumnId, SqlColumnIdentifying, SqlInterpolationParameter, UnOption}

import scala.annotation.implicitNotFound
import scala.language.implicitConversions
import scala.util.NotGiven

/**
 * Helper for going through the field path of Structure.
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
  def selectDynamic(name: String): ColumnPath[R, ?] = {
    structure.selectField(name) match {
      case None        =>
        throw new IllegalArgumentException(s"Unknown field ${name}, valid: [${structure.fieldNames.mkString(",")}]")
      case Some(found) =>
        ColumnPath.Child(found.structure, this, name, found.index)
    }
  }

  /** Build a getter for this field from the base type. */
  def buildGetter: R => T

  /** The structure of T */
  def structure: Structure[T]

  override def columnIds: Seq[SqlColumnId] = structure.columns.map(_.id)

  override final def toInterpolationParameter: SqlInterpolationParameter = columnIds

  override def toString: String = {
    columnIds match {
      case Seq(one) => one.toString
      case multiple => multiple.mkString("[", ",", "[")
    }
  }

  /** Prepend a path. */
  private[usql] def prepend[R2](columnPath: ColumnPath[R2, R]): ColumnPath[R2, T]

  private[usql] final def append[T2](columnPath: ColumnPath[T, T2]): ColumnPath[R, T2] = columnPath.prepend(this)
}

/** A Column Path which is at Type T */
type ColumnBasePath[T] = ColumnPath[?, T]

/** A Column path which is at the root of T */
type ColumnRootPath[T] = ColumnPath[T, T]

object ColumnPath {

  def make[T](using f: Structure[T]): ColumnPath[T, T] = Root(f)

  case class Root[T](structure: Structure[T]) extends ColumnPath[T, T] {
    override def prepend[R2](path: ColumnPath[R2, T]): ColumnPath[R2, T] = {
      path
    }

    override def buildGetter: T => T = identity
  }

  case class Child[R, P, T](structure: Structure[T], parent: ColumnPath[R, P], field: String, fieldIdx: Int)
      extends ColumnPath[R, T] {
    override def prepend[R2](path: ColumnPath[R2, R]): ColumnPath[R2, T] = {
      parent.prepend(path).selectDynamic(field).asInstanceOf[ColumnPath[R2, T]]
    }

    override def buildGetter: R => T = {
      val parentGetter = parent.buildGetter
      parentGetter.andThen { value =>
        parent.structure.split(value).apply(fieldIdx).asInstanceOf[T]
      }
    }
  }

  sealed trait TuplePath[R, T <: Tuple] extends ColumnPath[R, T] {
    override def structure: SqlFielded[T]

    override def prepend[R2](path: ColumnPath[R2, R]): TuplePath[R2, T]
  }

  case class EmptyTuplePath[R]() extends TuplePath[R, EmptyTuple] {
    override def prepend[R2](path: ColumnPath[R2, R]): TuplePath[R2, EmptyTuple] = EmptyTuplePath()

    override def structure: SqlFielded[EmptyTuple] = SqlFielded.emptyTuple

    override def buildGetter: R => EmptyTuple = _ => EmptyTuple
  }

  case class RecTuplePath[R, H, T <: Tuple](head: ColumnPath[R, H], tail: TuplePath[R, T])
      extends TuplePath[R, H *: T] {
    override def prepend[R2](path: ColumnPath[R2, R]): TuplePath[R2, H *: T] = RecTuplePath(
      head.prepend(path),
      tail.prepend(path)
    )

    override def structure: SqlFielded[H *: T] = SqlFielded.recursiveTuple(using head.structure, tail.structure)

    override def buildGetter: R => H *: T = {
      val headGetter = head.buildGetter
      val tailGetter = tail.buildGetter
      input => (headGetter(input) *: tailGetter(input))
    }
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

  /**
   * Wraps a `TuplePath` so the result type is a NamedTuple. Field access (`selectDynamic`) resolves on the named
   * labels; `buildGetter` reuses the underlying tuple path because `NamedTuple[N, VC]` and `VC` share runtime
   * representation.
   */
  case class NamedTupleColumnPath[R, N <: Tuple, VC <: Tuple](
      tuplePath: TuplePath[R, VC],
      structure: SqlFielded[NamedTuple.NamedTuple[N, VC]]
  ) extends ColumnPath[R, NamedTuple.NamedTuple[N, VC]] {

    override def buildGetter: R => NamedTuple.NamedTuple[N, VC] =
      tuplePath.buildGetter.asInstanceOf[R => NamedTuple.NamedTuple[N, VC]]

    override def prepend[R2](path: ColumnPath[R2, R]): ColumnPath[R2, NamedTuple.NamedTuple[N, VC]] =
      NamedTupleColumnPath(tuplePath.prepend(path), structure)
  }

  /**
   * Build a ColumnPath from a NamedTuple of Column Paths.
   *
   * `NotGiven[N =:= Tuple]` rejects the case where the user passed a plain tuple — Scala 3's NamedTuple opaque type has
   * lower bound `>: V`, so a regular tuple is statically assignable to `NamedTuple[Tuple, V]` and would otherwise make
   * this conversion ambiguous with [[fromTuple]].
   */
  implicit def fromNamedTuple[N <: Tuple, V <: Tuple, R, VC <: Tuple](
      in: NamedTuple.NamedTuple[N, V]
  )(
      using b: BuildFromTuple.Aux[V, VC, R],
      labels: SqlFielded.NamedTupleLabels[N],
      notPlainTuple: NotGiven[N =:= Tuple]
  ): ColumnPath[R, NamedTuple.NamedTuple[N, VC]] = {
    val tuplePath      = b.build(in.asInstanceOf[V])
    val namedStructure = SqlFielded.namedTupleFieldedFromLabels[N, VC](tuplePath.structure, labels.labels)
    NamedTupleColumnPath(tuplePath, namedStructure)
  }

  /**
   * Selectable view over a NamedTuple-typed `ColumnPath`.
   *
   * The parent [[ColumnPath.Fields]] type alias depends on a `T match { case Option[r] => ... }` that cannot reduce
   * when T is a NamedTuple — the opaque type isn't statically disjoint from `Option`, so Scala's structural lookup
   * can't find named fields directly via `path.fieldName`. This view declares `Fields` directly in terms of N and V,
   * sidestepping the optionality match (a NamedTuple is never optional itself).
   *
   * Reach it with [[NamedTupleViewOps.named]]: `q.filter(_.named.fieldName === ...)`.
   */
  trait NamedTupleFieldsView[R, N <: Tuple, V <: Tuple] extends Selectable {
    type Fields = NamedTuple.NamedTuple[N, scala.Tuple.Map[V, [X] =>> ColumnPath[R, X]]]

    def underlying: ColumnPath[R, NamedTuple.NamedTuple[N, V]]

    def selectDynamic(name: String): ColumnPath[R, ?] = underlying.selectDynamic(name)
  }

  extension [R, N <: Tuple, V <: Tuple](path: ColumnPath[R, NamedTuple.NamedTuple[N, V]])(
      using NotGiven[N =:= Tuple]
  )
    /** Open the path's named-tuple structure for typed field access (`path.named.fieldName`). */
    def named: NamedTupleFieldsView[R, N, V] = new NamedTupleFieldsView[R, N, V] {
      override def underlying: ColumnPath[R, NamedTuple.NamedTuple[N, V]] = path
    }
}
