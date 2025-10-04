package usql

/** Something which can produce an identifier. */
trait SqlColumnIdentifying {

  /** Returns column idenfitiers. */
  def columnIds: Seq[SqlColumnId]
}

/**
 * Identifies an Sql Column
 * @param name
 *   raw name
 * @param quoted
 *   if true, the identifier will be quoted.
 */
@throws[IllegalArgumentException]("If name contains a \"")
case class SqlColumnId(name: String, quoted: Boolean, alias: Option[String] = None) extends SqlColumnIdentifying {
  require(!name.contains("\""), "Identifiers may not contain \"")

  /** Serialize the identifier. */
  def serialize: String = {
    val sb = StringBuilder()
    alias.foreach { alias =>
      sb ++= alias
      sb += '.'
    }
    if quoted then {
      sb += '"'
    }
    sb ++= name
    if quoted then {
      sb += '"'
    }
    sb.result()
  }

  /** Placeholder for select query */
  def placeholder: SqlRawPart = SqlRawPart("?")

  /** Named placeholder for update query */
  def namedPlaceholder: SqlRawPart = SqlRawPart(serialize + " = ?")

  override def toString: String = serialize

  override def columnIds: Seq[SqlColumnId] = List(this)
}

object SqlColumnId {
  given stringToIdentifier: Conversion[String, SqlColumnId] with {
    override def apply(x: String): SqlColumnId = fromString(x)
  }

  def fromString(s: String): SqlColumnId = {
    if s.length >= 2 && s.startsWith("\"") && s.endsWith("\"") then {
      SqlColumnId(s.drop(1).dropRight(1), true)
    } else {
      if SqlReservedWords.isReserved(s) then {
        SqlColumnId(s, quoted = true)
      } else {
        SqlColumnId(s, quoted = false)
      }
    }
  }

  def fromStrings(s: String*): Seq[SqlColumnId] = s.map(fromString)
}
