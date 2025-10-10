package usql

import SqlInterpolationParameter.{InnerSql, SqlParameter}

import java.sql.{Connection, PreparedStatement}
import scala.annotation.{tailrec, targetName}
import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.Using

extension (sc: StringContext) {
  def sql(parameters: SqlInterpolationParameter*): Sql = {
    Sql(fixParameters(parameters))
  }

  /** Bring parameters into a canonical format. */
  private def fixParameters(parameters: Seq[SqlInterpolationParameter]): Seq[(String, SqlInterpolationParameter)] = {
    @tailrec
    def fix(
        parts: List[String],
        params: List[SqlInterpolationParameter],
        builder: List[(String, SqlInterpolationParameter)]
    ): List[(String, SqlInterpolationParameter)] = {
      (parts, params) match {
        case (Nil, _)                                                                          =>
          // No more parts
          builder
        case (part :: restParts, Nil) if part.isEmpty                                          =>
          // Skip it, empty part and no parameter
          fix(restParts, Nil, builder)
        case (part :: restParts, Nil)                                                          =>
          // More Parts but no parameters
          fix(restParts, Nil, (part, SqlInterpolationParameter.Empty) :: builder)
        case (part :: restParts, (param: SqlParameter[?]) :: restParams) if part.endsWith("#") =>
          // Getting #${..} parameters to work
          val replacedPart = part.stripSuffix("#") + param.dataType.serialize(param.value)
          fix(restParts, restParams, (replacedPart, SqlInterpolationParameter.Empty) :: builder)
        case (part :: restParts, (param: InnerSql) :: restParams)                              =>
          // Inner Sql

          val inner = if part.isEmpty then {
            Nil
          } else {
            List(part -> SqlInterpolationParameter.Empty)
          }

          val combined = param.sql.parts.toList.reverse ++ inner ++ builder
          fix(restParts, restParams, combined)
        case (part :: restParts, param :: restParams)                                          =>
          // Regular Case
          fix(restParts, restParams, (part, param) :: builder)
      }
    }
    fix(sc.parts.toList, parameters.toList, Nil).reverse
  }
}

/** SQL with already embedded parameters. */
case class Sql(parts: Seq[(String, SqlInterpolationParameter)]) extends SqlBase {

  /** Converts to SQL Text. */
  def sql: String = {
    val s = StringBuilder()
    serializeSql(s)
    s.result()
  }

  def serializeSql(s: StringBuilder): Unit = {
    parts.foreach { case (part, param) =>
      s ++= part
      param.serializeSql(s)
    }
  }

  override def toString: String = sql

  /** Simplify long random generated aliases. */
  def simplifyAliases: Sql = {
    val collected                            = collectAliases
    val builder: mutable.Map[String, String] = mutable.Map.empty
    val used: mutable.Set[String]            = mutable.Set.empty
    collected.foreach { alias =>
      // Try to keep the first character, its often better readable.
      val first = alias.take(1)
      if !used.contains(first) then {
        builder += (alias -> first)
        used += first
      } else {
        val toUse = (for
          i        <- (0 until 100).view
          candidate = first + i
          if !used.contains(candidate)
        yield (candidate)).headOption.getOrElse {
          throw new IllegalStateException(s"Could not find a candidate replacement for ${alias}")
        }
        builder += (alias -> toUse)
        used += toUse
      }
    }
    val mapping                              = builder.toMap
    mapAliases(mapping)
  }

  /** Collect aliases within this SQL */
  def collectAliases: Set[String] = parts.flatMap(_._2.collectAliases).toSet

  /** Map aliases to a new name. */
  def mapAliases(map: Map[String, String]): Sql = copy(
    parts = parts.map { case (const, param) =>
      const -> param.mapAliases(map)
    }
  )

  /** Collect embedded parameters. */
  def parameters: Seq[SqlParameter[?]] = parts.flatMap(_._2.parameters)

  override def withPreparedStatement[T](
      f: PreparedStatement => T
  )(using cp: ConnectionProvider, sp: StatementPreparator): T = {
    cp.withConnection {
      val c = summon[Connection]
      Using.resource(sp.prepare(c, sql)) { statement =>
        parameters.zipWithIndex.foreach { case (param, idx) =>
          param.dataType.fillByZeroBasedIdx(idx, statement, param.value)
        }
        f(statement)
      }
    }
  }

  def stripMargin: Sql = {
    stripMargin('|')
  }

  def stripMargin(marginChar: Char): Sql = {
    Sql(
      parts.map { case (s, p) =>
        s.stripMargin(marginChar) -> p
      }
    )
  }

  @targetName("concat")
  inline def +(other: Sql): Sql = concat(other)

  def concat(other: Sql): Sql = {
    Sql(
      this.parts ++ other.parts
    )
  }
}
