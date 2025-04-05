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
 * @param mapping
 *   the mapping which will be applied
 */
case class ColumnGroup(mapping: ColumnGroupMapping = ColumnGroupMapping.Pattern()) extends StaticAnnotation
