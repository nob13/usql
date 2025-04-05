package usql.dao

import usql.SqlIdentifier

import scala.annotation.StaticAnnotation

/** Annotation to override the default table name in [[SqlTabular]] */
case class TableName(name: String) extends StaticAnnotation

/** Annotation to override the default column name in [[SqlColumnar]] */
case class ColumnName(name: String) extends StaticAnnotation {
  def id: SqlIdentifier = SqlIdentifier.fromString(name)
}

/**
 * Controls the way nested column group names are generated.
 *
 * @param pattern
 *   the name pattern which will be applied. `%m` will be replaced by the member name, %c will be replaced by the child
 *   column name.
 */
case class ColumnGroup(pattern: String = "%m_%c") extends StaticAnnotation {

  /** Mapping for deriving names. */
  def mapping: ColumnGroupMapping = ColumnGroupMapping.Pattern(pattern)
}
