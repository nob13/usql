package usql.dao

import usql.{SqlIdentifier, SqlInterpolationParameter}

sealed trait TupleColumnPath[R, T <: Tuple] extends ColumnPath[R, T]

object TupleColumnPath {
  case class Empty[R]() extends TupleColumnPath[R, EmptyTuple] {
    override def selectDynamic(name: String): ColumnPath[R, _] = {
      throw new IllegalArgumentException("No fields in empty path")
    }

    override def ![X](using ev: (EmptyTuple) => Option[X]): ColumnPath[R, X] = {
      throw new IllegalArgumentException("No fields in empty path")
    }

    override def buildGetter: R => EmptyTuple = _ => EmptyTuple

    override def toInterpolationParameter: SqlInterpolationParameter = SqlInterpolationParameter.Empty

    override def buildIdentifier: Seq[SqlIdentifier] = Nil

    override def structure: SqlFielded[EmptyTuple] = emptyStructure
  }

  private val emptyStructure: SqlFielded[EmptyTuple] = SqlFielded.SimpleSqlFielded(
    Nil,
    _ => Nil,
    _ => EmptyTuple
  )

  case class Rec[R, H, T <: Tuple](head: ColumnPath[R, H],
                                   tail: ColumnPath[R, T]) extends TupleColumnPath[R, H *: T] {
    override def selectDynamic(name: String): ColumnPath[R, _] = {
      val index = name.stripPrefix("_").toIntOption.getOrElse {
        throw new IllegalStateException(s"Unknown field: ${name}")
      } - 1
      if index == 0 then {
        head
      } else {
        tail.selectDynamic(s"_${index}")
      }
    }

    override def ![X](using ev: H *: T => Option[X]): ColumnPath[R, X] = {
      throw new IllegalStateException("Should not come here")
    }

    override def buildGetter: R => H *: T = {
      val tailGetters = tail.buildGetter
      val headGetter = head.buildGetter
      x => {
        headGetter(x) *: tailGetters(x)
      }
    }

    override def toInterpolationParameter: SqlInterpolationParameter = buildIdentifier

    override def buildIdentifier: Seq[SqlIdentifier] = head.buildIdentifier ++ tail.buildIdentifier

    override def structure: SqlFielded[H *: T] = {
      val tailStructure = tail.structure
      SqlFielded.SimpleSqlFielded(
        fields = Field.Group(
          "_1",
          ColumnGroupMapping.Anonymous,
          SqlIdentifier.fromString("_1"),
          head.structure
        ) +: tailStructure.fields, // TODO: Umbenennung
        splitter = x => x.head :: tailStructure.split(x.tail).toList,
        builder = v => v.head.asInstanceOf[H] *: tailStructure.build(v.tail)
      )
    }
  }
}
