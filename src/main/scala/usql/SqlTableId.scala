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

object SqlTableId {
  def fromString(s: String): SqlTableId = {
    if s.length >= 2 && s.startsWith("\"") && s.endsWith("\"") then {
      SqlTableId(s.drop(1).dropRight(1), true)
    } else {
      if SqlReservedWords.isReserved(s) then {
        SqlTableId(s, quoted = true)
      } else {
        SqlTableId(s, quoted = false)
      }
    }
  }
}
