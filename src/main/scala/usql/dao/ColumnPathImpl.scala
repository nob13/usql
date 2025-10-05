package usql.dao

import usql.{Optionalize, SqlColumnId}

private[usql] abstract class ColumnPathAtFielded[R, T](
    fielded: SqlFielded[T],
    parentMapping: SqlColumnId => SqlColumnId
) extends ColumnPath[R, T] {
  override def selectDynamic(name: String): ColumnPath[R, _] = {
    val (field, fieldIdx) = fielded.fields.view.zipWithIndex.find(_._1.fieldName == name).getOrElse {
      throw new IllegalStateException(
        s"Unknown field ${name}, expected: one of ${fielded.fields.map(_.fieldName).mkString("[", ",", "]")}"
      )
    }
    selectField(name: String, field, fieldIdx)
  }

  private def selectField[X](name: String, field: Field[X], fieldIdx: Int): ColumnPath[R, X] = {
    val subGetter: T => X = (value) => {
      val splitted = fielded.split(value)
      splitted.apply(fieldIdx).asInstanceOf[X]
    }

    field match {
      case c: Field.Column[X] =>
        ColumnPathSelectColumn(name, c.column, childrenMapping, this, subGetter)
      case g: Field.Group[X]  =>
        ColumnPathSelectGroup(name, g, childrenMapping, this, subGetter)
    }
  }

  /** Returns the column name mapping of the current element. */
  protected def currentMapping: SqlColumnId => SqlColumnId = identity

  private def childrenMapping: SqlColumnId => SqlColumnId = {
    val pm = parentMapping
    val cm = currentMapping
    in => pm(cm(in))
  }

  override def structure: SqlFielded[T] = {
    SqlFielded.MappedSqlFielded(fielded, childrenMapping)
  }
}

private[usql] case class ColumnPathStart[R](fielded: SqlFielded[R])
    extends ColumnPathAtFielded[R, R](
      fielded,
      parentMapping = identity
    ) {

  override def buildGetter: R => R = identity

  override def columnIds: Seq[SqlColumnId] = fielded.columns.map(_.id)

  override def structure: SqlFielded[R] = fielded

  override def prepend[R2](columnPath: ColumnPath[R2, R]): ColumnPath[R2, R] = {
    columnPath
  }

  override def isEmpty: Boolean = true
}

private[usql] case class ColumnPathSelectGroup[R, P, T](
    selected: String,
    group: Field.Group[T],
    parentMapping: SqlColumnId => SqlColumnId,
    parent: ColumnPath[R, P],
    subGetter: P => T
) extends ColumnPathAtFielded[R, T](group.fielded, parentMapping) {

  override def columnIds: Seq[SqlColumnId] = {
    group.columns.map(c => parentMapping(c.id))
  }

  override protected def currentMapping: SqlColumnId => SqlColumnId = {
    group.mapChildColumnName
  }

  override def buildGetter: R => T = {
    val parentGetter = parent.buildGetter
    root => subGetter(parentGetter(root))
  }

  override def prepend[R2](columnPath: ColumnPath[R2, R]): ColumnPath[R2, T] = {
    parent.prepend(columnPath).selectDynamic(selected).asInstanceOf[ColumnPath[R2, T]]
  }
}

private[usql] case class ColumnPathSelectColumn[R, P, T](
    selected: String,
    column: SqlColumn[T],
    mapping: SqlColumnId => SqlColumnId,
    parent: ColumnPath[R, P],
    subGetter: P => T
) extends ColumnPath[R, T] {
  override def selectDynamic(name: String): ColumnPath[R, _] = {
    throw new IllegalStateException(s"Can walk further column")
  }

  override def buildGetter: R => T = {
    val parentGetter = parent.buildGetter
    root => subGetter(parentGetter(root))
  }

  override def structure: SqlFielded[T] | SqlColumn[T] = {
    column.copy(
      id = mapping(column.id)
    )
  }

  override def columnIds: Seq[SqlColumnId] = {
    Seq(mapping(column.id))
  }

  override def prepend[R2](columnPath: ColumnPath[R2, R]): ColumnPath[R2, T] = {
    parent.prepend(columnPath).selectDynamic(selected).asInstanceOf[ColumnPath[R2, T]]
  }
}

private[usql] case class ColumnPathOptionalize[R, T](underlying: ColumnPath[R, T])
    extends ColumnPath[R, Optionalize[T]] {
  lazy val underlyingIsOptional = underlying.structure.isOptional

  override def selectDynamic(name: String): ColumnPath[R, _] = {
    ColumnPathOptionalize(
      underlying.selectDynamic(name)
    )
  }

  override def buildGetter: R => Optionalize[T] = {
    val underlyingGetter = underlying.buildGetter
    if underlyingIsOptional then { it =>
      underlyingGetter(it).asInstanceOf[Optionalize[T]]
    } else { it =>
      Some(underlyingGetter(it)).asInstanceOf[Optionalize[T]]
    }
  }

  override def structure: SqlFielded[Optionalize[T]] | SqlColumn[Optionalize[T]] = {
    underlying.structure match {
      case f: SqlFielded[T] => f.optionalize
      case c: SqlColumn[T]  => c.optionalize
    }
  }

  override def columnIds: Seq[SqlColumnId] = {
    underlying.columnIds
  }

  override def prepend[R2](columnPath: ColumnPath[R2, R]): ColumnPath[R2, Optionalize[T]] = {
    ColumnPathOptionalize(underlying.prepend(columnPath))
  }
}

private[usql] object ColumnPathOptionalize {
  def make[R, T](underlying: ColumnPath[R, T]): ColumnPathOptionalize[R, T] = underlying match {
    case c: ColumnPathOptionalize[R, ?] => c.asInstanceOf[ColumnPathOptionalize[R, T]]
    case otherwise                      => ColumnPathOptionalize(otherwise)
  }
}
