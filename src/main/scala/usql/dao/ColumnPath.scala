package usql.dao

import usql.SqlIdentifier

case class ColumnPath[T](root: SqlFielded[?], fields: List[String]) extends Selectable {
  type Fields = NamedTuple.Map[NamedTuple.From[T], ColumnPath]

  def selectDynamic(name: String): ColumnPath[?] = {
    ColumnPath(root, name :: fields)
  }

  def id: SqlIdentifier = {
    val reversed = fields.reverse
    val walked   = reversed.foldLeft(ColumnPath.FieldedWalker(root): ColumnPath.Walker)(_.select(_))
    walked.id
  }
}

object ColumnPath {
  trait Walker {
    def select(field: String): Walker
    def id: SqlIdentifier
  }

  case class FieldedWalker(model: SqlFielded[?]) extends Walker {
    override def select(field: String): Walker = {
      model.fields
        .collectFirst {
          case f: Field.Column[?] if f.fieldName == field =>
            ColumnWalker(f)
          case f: Field.Group[?] if f.fieldName == field  =>
            FieldedWalker(f.fielded)
        }
        .getOrElse {
          throw new IllegalStateException(s"Can not fiend field nane ${field}")
        }
    }

    override def id: SqlIdentifier = {
      throw new IllegalStateException("Not at a final field")
    }
  }

  case class ColumnWalker(column: Field.Column[?]) extends Walker {
    override def select(field: String): Walker = {
      throw new IllegalStateException(s"Can walk further column")
    }

    override def id: SqlIdentifier = column.column.id
  }
}
