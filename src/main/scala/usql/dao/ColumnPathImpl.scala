package usql.dao

import usql.dao.ColumnPathImpl.{FieldedWalker, Walker}
import usql.{SqlIdentifier, SqlInterpolationParameter}

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
    Seq(walker.id.copy(alias = alias))
  }

  override def toInterpolationParameter: SqlInterpolationParameter = buildIdentifier

  override def buildGetter: R => T = {
    walker.get
  }
}

private[usql] object ColumnPathImpl {
  trait Walker[R, T] {
    def select(field: String): Walker[R, ?]

    def id: SqlIdentifier

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

    override def id: SqlIdentifier = {
      throw new IllegalStateException("Not at a final field")
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

    override def id: SqlIdentifier = mapping(column.column.id)

    override def get(root: R): T = getter(root)
  }
}
