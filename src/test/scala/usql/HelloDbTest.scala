package usql

import usql.util.TestBaseWithH2

import java.sql.SQLException

class HelloDbTest extends TestBaseWithH2 {

  override protected def baseSql: String =
    """
      |CREATE TABLE "user" (id INT PRIMARY KEY, name VARCHAR);
      |""".stripMargin

  val tableName = SqlColumnId.fromString("user")

  it should "work" in {
    sql"""INSERT INTO "user" (id, name) VALUES (${1}, ${"Hello World"})""".update.run()
    sql"""INSERT INTO "user" (id, name) VALUES (${3}, ${"How are you?"})""".update.run()

    withClue("it should be possible to build various result row parsers") {
      summon[RowDecoder[EmptyTuple]]
      summon[RowDecoder[Int *: EmptyTuple]]
      summon[RowDecoder[Int]]
      summon[RowDecoder[(Int, String)]]
    }

    sql"""SELECT id, name FROM "user" WHERE id=${1}""".query[(Int, String)].one() shouldBe Some(1 -> "Hello World")

    sql"""SELECT id, name FROM "user" WHERE id=${2}""".query[(Int, String)].one() shouldBe None

    sql"""SELECT id, name FROM "user" ORDER BY id""".query[(Int, String)].all() shouldBe Seq(
      1 -> "Hello World",
      3 -> "How are you?"
    )

    withClue("It should allow inferenced return types") {
      val result: Seq[(Int, String)] = sql"""SELECT id, name FROM "user" ORDER BY id""".queryAll()
      result shouldBe Seq(
        1 -> "Hello World",
        3 -> "How are you?"
      )
    }
  }

  it should "allow hash replacements" in {
    sql"""SELECT id, name FROM #${"\"user\""} WHERE id=${1}""".queryOne[(Int, String)]() shouldBe empty
  }

  it should "allow identifiers" in {
    val userTable = SqlColumnId.fromString("user")
    sql"""SELECT id, name FROM ${userTable}""".queryOne[(Int, String)]() shouldBe empty
  }

  it should "allow batch inserts" in {
    val batchInsert = sql"""INSERT INTO "user" (id, name) VALUES(?,?)""".batch(
      Seq(
        1 -> "Hello",
        2 -> "World"
      )
    )
    val response    = batchInsert.run()
    response shouldBe Seq(1, 1)

    val got = sql"""SELECT id, name FROM "user" ORDER BY ID""".queryAll[(Int, String)]()
    got shouldBe Seq(
      1 -> "Hello",
      2 -> "World"
    )
  }

  it should "allow transactions" in {
    val insertCall = sql"INSERT INTO ${tableName} (id, name) VALUES(${1}, ${"Alice"})"
    intercept[SQLException] {
      transaction {
        insertCall.execute()
        insertCall.execute()
      }
    }
    sql"SELECT COUNT(*) FROM ${tableName}".queryOne[Int]() shouldBe Some(0)

    transaction {
      insertCall.execute()
    }

    sql"SELECT COUNT(*) FROM ${tableName}".queryOne[Int]() shouldBe Some(1)
  }

  it should "allow in queries" in {
    sql"""INSERT INTO "user" (id, name) VALUES (${1}, ${"Alice"})""".update.run()
    sql"""INSERT INTO "user" (id, name) VALUES (${3}, ${"Bob"})""".update.run()

    val ids = Seq(1, 2, 3)

    val got =
      sql"""
        SELECT name FROM "user" WHERE id IN (${SqlParameters(ids)})
         """.queryAll[String]()

    got should contain theSameElementsAs Seq("Alice", "Bob")

    sql"""
            SELECT name FROM "user" WHERE id IN (${SqlParameters(Seq(9, 8, 7, 6))})
             """.queryAll[String]() shouldBe empty

    sql"""
                SELECT name FROM "user" WHERE id IN (${SqlParameters(Nil: Seq[Int])})
                 """.queryAll[String]() shouldBe empty
  }
}
