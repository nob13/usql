package usql.util

import org.scalatest.{BeforeAndAfterEach, Suite}

import java.sql.{Connection, DriverManager}
import scala.util.{Random, Using}

/** Testbase which provides an empty postgres database per Test. */
trait PostgresSupport extends TestDatabaseSupport with BeforeAndAfterEach {
  self: Suite =>

  private var _dbName: Option[String] = None

  protected def dbName: String = {
    _dbName.getOrElse {
      throw new IllegalStateException(s"DB name not initialized?!")
    }
  }

  override protected def beforeEach(): Unit = {
    _dbName = Some(s"unittest_${Random.nextLong()}")
    withRootConnection { con =>
      con.prepareStatement("CREATE DATABASE ${dbName}").execute()
    }
    super.beforeEach()
  }

  override def makeJdbcUrl(): String = {
    s"jdbc:postgresql:${dbName}"
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    withRootConnection { con =>
      con.prepareStatement(s"DROP DATABASE ${dbName}")
    }
  }

  private def withRootConnection[T](fn: Connection => T): T = {
    Using.resource(DriverManager.getConnection("jdbc:postgresql:postgres")) { con =>
      fn(con)
    }
  }
}
