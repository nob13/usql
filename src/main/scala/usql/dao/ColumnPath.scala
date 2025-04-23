package usql.dao

import usql.{SqlIdentifier, SqlIdentifying}

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
case class ColumnPath[R, T](root: SqlFielded[R], fields: List[String] = Nil, alias: Option[String] = None)
    extends Selectable
    with SqlIdentifying {

  final type Child[X] = ColumnPath[R, X]

  type Fields = NamedTuple.Map[NamedTuple.From[T], Child]

  def selectDynamic(name: String): ColumnPath[R, ?] = {
    ColumnPath(root, name :: fields, alias)
  }

  private lazy val walker: ColumnPath.Walker[R, T] = {
    val reversed = fields.reverse
    reversed
      .foldLeft(
        ColumnPath.FieldedWalker[R, R](
          root,
          mapping = identity,
          getter = identity
        ): ColumnPath.Walker[?, ?]
      )(_.select(_))
      .asInstanceOf[ColumnPath.Walker[R, T]]
  }

  override def buildIdentifier: SqlIdentifier = {
    walker.id.copy(alias = alias)
  }

  def buildGetter: R => T = {
    walker.get
  }
}

object ColumnPath {

  def make[T](using f: SqlFielded[T]): ColumnPath[T, T] = ColumnPath(f)

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
