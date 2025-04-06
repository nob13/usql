package usql

/**
 * An SQL Identifier (table or colum name
 * @param name
 *   raw name
 * @param quoted
 *   if true, the identifier will be quoted.
 */
@throws[IllegalArgumentException]("If name contains a \"")
case class SqlIdentifier(name: String, quoted: Boolean) {
  require(!name.contains("\""), "Identifiers may not contain \"")

  /** Serialize the identifier. */
  def serialize: String = {
    if quoted then {
      "\"" + name + "\""
    } else {
      name
    }
  }

  /** Placeholder for select query */
  def placeholder: SqlRawPart = SqlRawPart("?")

  /** Named placeholder for update query */
  def namedPlaceholder: SqlRawPart = SqlRawPart(serialize + " = ?")

  override def toString: String = serialize
}

object SqlIdentifier {
  given stringToIdentifier: Conversion[String, SqlIdentifier] with {
    override def apply(x: String): SqlIdentifier = fromString(x)
  }

  def fromString(s: String): SqlIdentifier = {
    if s.length >= 2 && s.startsWith("\"") && s.endsWith("\"") then {
      SqlIdentifier(s.drop(1).dropRight(1), true)
    } else {
      if SqlReservedWords.isReserved(s) then {
        SqlIdentifier(s, quoted = true)
      } else {
        SqlIdentifier(s, quoted = false)
      }
    }
  }
}
