package usql.dao

import usql.SqlColumnId

/** Maps an inner column name inside a ColumnGroup. */
trait ColumnGroupMapping {
  def map(columnBaseName: SqlColumnId, childId: SqlColumnId): SqlColumnId
}

object ColumnGroupMapping {

  /** Simple Pattern based column group mapping. */
  case class Pattern(pattern: String = "%m_%c") extends ColumnGroupMapping {
    override def map(columnBaseName: SqlColumnId, childId: SqlColumnId): SqlColumnId = {
      val applied = pattern
        .replace("%m", columnBaseName.name)
        .replace("%c", childId.name)
      // Do not take escaping from the field or parent as this can lead to strange situations (still hacky)
      SqlColumnId.fromString(applied)
    }
  }

  case object Anonymous extends ColumnGroupMapping {
    override def map(columnBaseName: SqlColumnId, childId: SqlColumnId): SqlColumnId = childId
  }

  /** Maps identifiers to something else. */
  case class Mapped(underlying: ColumnGroupMapping, mapping: SqlColumnId => SqlColumnId) extends ColumnGroupMapping {
    override def map(columnBaseName: SqlColumnId, childId: SqlColumnId): SqlColumnId = {
      mapping(underlying.map(columnBaseName, childId))
    }
  }
}
