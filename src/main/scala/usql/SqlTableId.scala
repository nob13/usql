package usql

/**
 * Identifies an Sql Table
 * @param name
 *   raw name
 * @param quoted
 *   if true, the identifier will be quoted.
 */
@throws[IllegalArgumentException]("If name contains a \"")
case class SqlTableId(name: String, quoted: Boolean) {
  require(!name.contains("\""), "Identifiers may not contain \"")

  /** Serialize the identifier. */
  def serialize: String = {
    val sb = StringBuilder()
    if quoted then {
      sb += '"'
    }
    sb ++= name
    if quoted then {
      sb += '"'
    }
    sb.result()
  }

  override def toString: String = serialize
}
