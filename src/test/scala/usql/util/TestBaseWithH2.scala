package usql.util

import usql.profiles.H2Profile

import scala.util.Random

trait H2Support extends TestDatabaseSupport {

  override def makeJdbcUrl(): String = {
    val name = "db" + Math.abs(Random.nextLong())
    classOf[org.h2.Driver].toString
    s"jdbc:h2:mem:${name};DB_CLOSE_DELAY=-1"
  }
}

/** Testbase which provides an empty H2 Database on every test. */
abstract class TestBaseWithH2 extends TestBaseWithDatabase with H2Support with H2Profile
