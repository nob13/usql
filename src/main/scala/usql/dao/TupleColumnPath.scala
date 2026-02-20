package usql.dao

import usql.SqlColumnId

import scala.annotation.implicitNotFound

sealed trait TupleColumnPath[R, T <: Tuple] extends ColumnPath[R, T] {
  final override def structure: SqlFielded[T] = structureAt(1)

  def structureAt(tupleIdx: Int): SqlFielded[T]

  override def prepend[R2](columnPath: ColumnPath[R2, R]): TupleColumnPath[R2, T]

  final override def selectDynamic(name: String): ColumnPath[R, ?] = {
    val idx = name.stripPrefix("_").toIntOption.getOrElse {
      throw new IllegalArgumentException(s"Unknown field: ${name}")
    } - 1
    selectIdx(idx)
  }

  protected def selectIdx(idx: Int): ColumnPath[R, ?]
}

object TupleColumnPath {
  case class Empty[R]() extends TupleColumnPath[R, EmptyTuple] {

    override def buildGetter: R => EmptyTuple = _ => EmptyTuple

    override def columnIds: Seq[SqlColumnId] = Nil

    override def structureAt(tupleIdx: Int): SqlFielded[EmptyTuple] = emptyStructure

    override def prepend[R2](columnPath: ColumnPath[R2, R]): TupleColumnPath[R2, EmptyTuple] = {
      Empty[R2]()
    }

    override protected def selectIdx(idx: Int): ColumnPath[R, ?] = {
      throw new IllegalArgumentException("No fields in empty path")
    }
  }

  private val emptyStructure: SqlFielded[EmptyTuple] = SqlFielded.SimpleSqlFielded(
    Nil,
    _ => Nil,
    _ => EmptyTuple
  )

  case class Rec[R, H, T <: Tuple](head: ColumnPath[R, H], tail: TupleColumnPath[R, T])
      extends TupleColumnPath[R, H *: T] {
    override protected def selectIdx(idx: Int): ColumnPath[R, ?] = {
      idx match {
        case 0 => head
        case n => tail.selectIdx(n - 1)
      }
    }

    override def buildGetter: R => H *: T = {
      val tailGetters = tail.buildGetter
      val headGetter  = head.buildGetter
      x => {
        headGetter(x) *: tailGetters(x)
      }
    }

    override def columnIds: Seq[SqlColumnId] = head.columnIds ++ tail.columnIds

    override def structureAt(tupleIdx: Int): SqlFielded[H *: T] = {
      val tailStructure = tail.structureAt(tupleIdx + 1)
      SqlFielded.SimpleSqlFielded(
        fields = headField(tupleIdx) +: tailStructure.fields,
        splitter = x => x.head :: tailStructure.split(x.tail).toList,
        builder = v => v.head.asInstanceOf[H] *: tailStructure.build(v.tail)
      )
    }

    private def headField(tupleIdx: Int): Field[T] = {
      head.structure match {
        case c: SqlColumn[T] @unchecked  =>
          Field.Column(
            s"_${tupleIdx}",
            c
          )
        case f: SqlFielded[T] @unchecked =>
          Field.Group(
            s"_${tupleIdx}",
            ColumnGroupMapping.Anonymous,
            SqlColumnId.fromString(s"_${tupleIdx}"),
            f
          )
      }
    }

    override def prepend[R2](columnPath: ColumnPath[R2, R]): TupleColumnPath[R2, H *: T] = {
      Rec(
        head = head.prepend(columnPath),
        tail = tail.prepend(columnPath)
      )
    }
  }

  /** Helper for building ColumnPath from Tuple */
  @implicitNotFound("Could not find BuildFromTuple")
  trait BuildFromTuple[T] {
    type CombinedType <: Tuple

    type Root

    def build(from: T): TupleColumnPath[Root, CombinedType]
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

        override def build(from: EmptyTuple): TupleColumnPath[R, EmptyTuple] = Empty[R]()
      }

    given recursive[H, T <: Tuple, R, TC <: Tuple](
        using tailBuild: BuildFromTuple.Aux[T, TC, R]
    ): BuildFromTuple.Aux[
      (ColumnPath[R, H] *: T),
      H *: TC,
      R
    ] = new BuildFromTuple[ColumnPath[R, H] *: T] {
      override type CombinedType = H *: TC

      override type Root = R

      override def build(from: (ColumnPath[R, H] *: T)): TupleColumnPath[R, CombinedType] =
        TupleColumnPath.Rec(from.head, tailBuild.build(from.tail))
    }
  }
}
