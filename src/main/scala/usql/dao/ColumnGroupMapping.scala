package usql.dao

import usql.SqlIdentifier

/** Maps an inner column name inside a ColumnGroup. */
trait ColumnGroupMapping {
  def map(columnBaseName: SqlIdentifier, childId: SqlIdentifier): SqlIdentifier
}

object ColumnGroupMapping {

  /** Simple Pattern based column group mapping. */
  case class Pattern(pattern: String = "%m_%c") extends ColumnGroupMapping {
    override def map(columnBaseName: SqlIdentifier, childId: SqlIdentifier): SqlIdentifier = {
      val applied = pattern
        .replace("%m", columnBaseName.name)
        .replace("%c", childId.name)
      // Do not take escaping from the field or parent as this can lead to strange situations (still hacky)
      SqlIdentifier.fromString(applied)
    }
  }

  case object Anonymous extends ColumnGroupMapping {
    override def map(columnBaseName: SqlIdentifier, childId: SqlIdentifier): SqlIdentifier = childId
  }
}
