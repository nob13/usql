package usql.dao

import usql.Optionalize

/** Base trait for Structures. */
trait Structure[T] {

  /** Returns the columns of this structure. */
  def columns: Seq[SqlColumn[?]]

  /** Returns the structure of this value optionalized. */
  def optionalize: Structure[Optionalize[T]]

  /** Select a field, maps columns accordingly */
  def selectField(name: String): Option[Structure[?]]

  /** Returns available field names. */
  def fieldNames: Seq[String]

  /** Converts into a field of given name (maybe with anonymous column mapping) */
  def toField(fieldName: String): Field[T]

  /** Split an instance into its fields, columns are just one */
  protected[dao] def split(value: T): Seq[Any]

  /** Build from field values, columns are just one */
  protected[dao] def build(fieldValues: Seq[Any]): T

  /** Cardinality for split and build. */
  protected[dao] def fieldCardinality: Int
}
