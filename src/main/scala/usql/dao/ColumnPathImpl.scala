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
    mapping: SqlIdentifier => SqlIdentifier,
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
    val newGetter: R => X = getter.andThen(subGetter)

    field match {
      case c: Field.Column[X] =>
        ColumnPathSelectColumn(c.column, mapping, this, newGetter)
      case g: Field.Group[X]  =>
        val newMapping: SqlIdentifier => SqlIdentifier =
          in => mapping(g.mapping.map(g.columnBaseName, in))
        println(s"Entering ${name}, previous: xxx => ${mapping(SqlIdentifier.fromString("xxx"))}, newMapping: xxx => ${newMapping(SqlIdentifier.fromString("xxx"))}")
        ColumnPathSelectGroup(g, newMapping, this, newGetter)
    }
  }

  override def ![X](using ev: T => Option[X]): ColumnPath[R, X] = ???

  override def structure: SqlFielded[T] = fielded

  override def buildGetter: R => T = getter
}

private[usql] case class ColumnPathStart[R](fielded: SqlFielded[R])
    extends ColumnPathAtFielded[R, R](
      fielded,
      mapping = identity,
      getter = identity
    ) {

  override def buildGetter: R => R = identity

  override def buildIdentifier: Seq[SqlIdentifier] = fielded.columns.map(_.id)
}

private[usql] case class ColumnPathSelectGroup[R, P, T](
    group: Field.Group[T],
    mapping: SqlIdentifier => SqlIdentifier,
    parent: ColumnPath[R, P],
    getter: R => T
) extends ColumnPathAtFielded[R, T](group.fielded, mapping, getter) {

  override def buildIdentifier: Seq[SqlIdentifier] = {
    group.columns.map(c => mapping(c.id))
  }
}

private[usql] case class ColumnPathSelectColumn[R, P, T](
    column: SqlColumn[T],
    mapping: SqlIdentifier => SqlIdentifier,
    parent: ColumnPath[R, P],
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

  override def buildIdentifier: Seq[SqlIdentifier] = Seq(mapping(column.id))
}

/*
private[usql] case class ColumnPathImpl[R, T](
    root: SqlFielded[R],
    fields: List[String] = Nil,
    alias: Option[String] = None
) extends ColumnPath[R, T] {

  def selectDynamic(name: String): ColumnPath[R, ?] = {
    ColumnPathImpl(root, name :: fields, alias)
  }

  override def ![X](using ev: T => Option[X]): ColumnPath[R, X] = {
    ColumnPathImpl(root, "!" :: fields, alias)
  }

  private lazy val walker: Walker[R, T] = {
    val reversed = fields.reverse
    reversed
      .foldLeft(
        FieldedWalker[R, R](
          root,
          mapping = identity,
          getter = identity
        ): Walker[?, ?]
      )(_.select(_))
      .asInstanceOf[Walker[R, T]]
  }

  override def buildIdentifier: Seq[SqlIdentifier] = {
    walker.ids.map(_.copy(alias = alias))
  }

  override def toInterpolationParameter: SqlInterpolationParameter = buildIdentifier

  override def buildGetter: R => T = {
    walker.get
  }

  override def structure: SqlFielded[T] = {
    ???
  }
}

private[usql] object ColumnPathImpl {
  trait Walker[R, T] {
    def select(field: String): Walker[R, ?]

    def ids: Seq[SqlIdentifier]

    def get(root: R): T
  }

  case class FieldedWalker[R, T](
      model: SqlFielded[T],
      mapping: SqlIdentifier => SqlIdentifier = identity,
      getter: R => T = identity
  ) extends Walker[R, T] {
    override def select(field: String): Walker[R, ?] = {
      model.fields.view.zipWithIndex
        .collectFirst {
          case (f, idx) if f.fieldName == field =>
            selectField(idx, f)
        }
        .getOrElse {
          throw new IllegalStateException(s"Can not fiend field nane ${field}")
        }
    }

    private def selectField[X](idx: Int, f: Field[X]): Walker[R, X] = {
      val subGetter: T => X  = (value) => {
        val splitted = model.split(value)
        splitted.apply(idx).asInstanceOf[X]
      }
      val newFetcher: R => X = getter.andThen(subGetter)
      f match {
        case f: Field.Column[X] => ColumnWalker[R, X](f, mapping, newFetcher)
        case g: Field.Group[X]  =>
          val subMapping: SqlIdentifier => SqlIdentifier = in => mapping(g.mapping.map(g.columnBaseName, in))
          FieldedWalker(g.fielded, subMapping, newFetcher)
      }
    }

    override def ids: Seq[SqlIdentifier] = {
      model.columns.map { column =>
        mapping(column.id)
      }
    }

    override def get(root: R): T = {
      getter(root)
    }
  }

  case class ColumnWalker[R, T](
      column: Field.Column[T],
      mapping: SqlIdentifier => SqlIdentifier = identity,
      getter: R => T
  ) extends Walker[R, T] {
    override def select(field: String): Walker[R, ?] = {
      throw new IllegalStateException(s"Can walk further column")
    }

    override def ids: Seq[SqlIdentifier] = Seq(mapping(column.column.id))

    override def get(root: R): T = getter(root)
  }
}
 */
