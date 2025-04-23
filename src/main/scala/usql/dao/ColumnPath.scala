package usql.dao

import usql.{SqlIdentifier, SqlIdentifying}

/**
 * Helper for going through the field path of SqlFielded.
 */
case class ColumnPath[R, T](root: SqlFielded[R], fields: List[String], alias: Option[String] = None)
    extends Selectable
    with SqlIdentifying {

  final type Child[X] = ColumnPath[R, X]

  type Fields = NamedTuple.Map[NamedTuple.From[T], Child]

  def selectDynamic(name: String): ColumnPath[R, ?] = {
    ColumnPath(root, name :: fields, alias)
  }

  override def buildIdentifier: SqlIdentifier = {
    val reversed = fields.reverse
    val walked   = reversed.foldLeft(ColumnPath.FieldedWalker(root): ColumnPath.Walker)(_.select(_))
    walked.id.copy(alias = alias)
  }
}

object ColumnPath {

  trait Walker {
    def select(field: String): Walker
    def id: SqlIdentifier
  }

  case class FieldedWalker(model: SqlFielded[?], mapping: SqlIdentifier => SqlIdentifier = identity) extends Walker {
    override def select(field: String): Walker = {
      model.fields
        .collectFirst {
          case f: Field.Column[?] if f.fieldName == field =>
            ColumnWalker(f, mapping)
          case f: Field.Group[?] if f.fieldName == field  =>
            val subMapping: SqlIdentifier => SqlIdentifier = in => mapping(f.mapping.map(f.columnBaseName, in))
            FieldedWalker(f.fielded, subMapping)
        }
        .getOrElse {
          throw new IllegalStateException(s"Can not fiend field nane ${field}")
        }
    }

    override def id: SqlIdentifier = {
      throw new IllegalStateException("Not at a final field")
    }
  }

  case class ColumnWalker(column: Field.Column[?], mapping: SqlIdentifier => SqlIdentifier = identity) extends Walker {
    override def select(field: String): Walker = {
      throw new IllegalStateException(s"Can walk further column")
    }

    override def id: SqlIdentifier = mapping(column.column.id)
  }
}
