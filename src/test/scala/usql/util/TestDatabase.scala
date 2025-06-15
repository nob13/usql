package usql.util

import org.scalatest.BeforeAndAfterEach
import usql.ConnectionProvider

import java.sql.{Connection, DriverManager}
import scala.util.{Random, Using}

trait TestDatabaseSupport {

  /** Generates a connection to the database. */
  def makeJdbcUrl(): String
}

trait TestDatabase extends BeforeAndAfterEach with TestDatabaseSupport {
  self: TestBase =>

  protected def baseSql: String = ""

  private var _rootConnection: Option[Connection] = None
  private var _url: Option[String]                = None

  protected def jdbcUrl: String = _url.getOrElse {
    throw new IllegalStateException(s"No connection")
  }

  given cp: ConnectionProvider with {
    override def withConnection[T](f: Connection ?=> T): T = {
      Using.resource(DriverManager.getConnection(jdbcUrl)) { c =>
        f(using c)
      }
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val url = makeJdbcUrl()

    val connection = DriverManager.getConnection(url)
    _rootConnection = Some(connection)
    _url = Some(url)

    runBaseSql()
  }

  override protected def afterEach(): Unit = {
    _rootConnection.foreach(_.close())
    super.afterEach()
  }

  protected def runSql(sql: String): Unit = {
    cp.withConnection {
      val c = summon[Connection]
      c.prepareStatement(sql).execute()
    }
  }

  protected def runSqlMultiline(sql: String): Unit = {
    val splitted = splitSql(baseSql)
    splitted.foreach { line =>
      runSql(line)
    }
  }

  private def runBaseSql(): Unit = {
    runSqlMultiline(baseSql)
  }

  protected def splitSql(s: String): Seq[String] = {
    // Note: very rough
    s.split("(?<=;)\\s+").toSeq.map(_.trim.stripSuffix(";")).filter(_.nonEmpty)
  }

}
