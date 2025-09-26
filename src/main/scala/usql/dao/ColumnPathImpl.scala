package usql.dao

import usql.{SqlIdentifier, SqlInterpolationParameter}

private[usql] case class ColumnPathAlias[R, T](underlying: ColumnPath[R, T], alias: String) extends ColumnPath[R, T] {
  override def selectDynamic(name: String): ColumnPath[R, _] = {
    copy(
      underlying = underlying.selectDynamic(name)
    )
  }

  override def ![X](using ev: T => Option[X]): ColumnPath[R, X] = {
    copy(
      underlying = underlying.!
    )
  }

  override def buildGetter: R => T = {
    underlying.buildGetter
  }

  override def structure: SqlFielded[T] | SqlColumn[T] = {
    // So correct?
    underlying.structure
  }

  override def buildIdentifier: Seq[SqlIdentifier] = {
    underlying.buildIdentifier.map(_.copy(alias = Some(alias)))
  }
}

private[usql] abstract class ColumnPathAtFielded[R, T](
    fielded: SqlFielded[T],
    parentMapping: SqlIdentifier => SqlIdentifier,
    getter: R => T
) extends ColumnPath[R, T] {
  override def selectDynamic(name: String): ColumnPath[R, _] = {
    val (field, fieldIdx) = fielded.fields.view.zipWithIndex.find(_._1.fieldName == name).getOrElse {
      throw new IllegalStateException(s"Unknown field ${name}")
    }
    selectField(name: String, field, fieldIdx)
  }

  private def selectField[X](name: String, field: Field[X], fieldIdx: Int): ColumnPath[R, X] = {
    val subGetter: T => X = (value) => {
      val splitted = fielded.split(value)
      splitted.apply(fieldIdx).asInstanceOf[X]
    }

    val newGetter: R => X                          = getter.andThen(subGetter)
    val newMapping: SqlIdentifier => SqlIdentifier = { in =>
      parentMapping(currentMapping(in))
    }

    field match {
      case c: Field.Column[X] =>
        ColumnPathSelectColumn(c.column, newMapping, this, newGetter)
      case g: Field.Group[X]  =>
        ColumnPathSelectGroup(g, newMapping, this, newGetter)
    }
  }

  /** Returns the column name mapping of the current element. */
  protected def currentMapping: SqlIdentifier => SqlIdentifier = identity

  override def ![X](using ev: T => Option[X]): ColumnPath[R, X] = ???

  override def structure: SqlFielded[T] = fielded

  override def buildGetter: R => T = getter
}

private[usql] case class ColumnPathStart[R](fielded: SqlFielded[R])
    extends ColumnPathAtFielded[R, R](
      fielded,
      parentMapping = identity,
      getter = identity
    ) {

  override def buildGetter: R => R = identity

  override def buildIdentifier: Seq[SqlIdentifier] = fielded.columns.map(_.id)
}

private[usql] case class ColumnPathSelectGroup[R, P, T](
    group: Field.Group[T],
    parentMapping: SqlIdentifier => SqlIdentifier,
    parent: ColumnPath[R, P],
    getter: R => T
) extends ColumnPathAtFielded[R, T](group.fielded, parentMapping, getter) {

  override def buildIdentifier: Seq[SqlIdentifier] = {
    group.columns.map(c => parentMapping(c.id))
  }

  override protected def currentMapping: SqlIdentifier => SqlIdentifier = {
    group.mapChildColumnName
  }
}

private[usql] case class ColumnPathSelectColumn[R, P, T](
    column: SqlColumn[T],
    mapping: SqlIdentifier => SqlIdentifier,
    parentMapping: ColumnPath[R, P],
    getter: R => T
) extends ColumnPath[R, T] {
  override def selectDynamic(name: String): ColumnPath[R, _] = {
    throw new IllegalStateException(s"Can walk further column")
  }

  override def ![X](using ev: T => Option[X]): ColumnPath[R, X] = {
    throw new NotImplementedError(s"Unacking option is not implemented here!")
  }

  override def buildGetter: R => T = getter

  override def structure: SqlFielded[T] | SqlColumn[T] = column

  override def buildIdentifier: Seq[SqlIdentifier] = {
    Seq(mapping(column.id))
  }
}
