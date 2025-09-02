package usql.dao

import usql.util.TestBase
import usql.profiles.PostgresProfile.*
import usql.sql

class QueryBuilderTest extends TestBase {
  case class Person(
      id: Int,
      name: String,
      age: Int
  ) derives SqlTabular

  object Person extends KeyedCrudBase[Int, Person] {
    override def key: KeyColumnPath = cols.id

    override lazy val tabular: SqlTabular[Person] = summon
  }

  it should "make a simple query" in {
    val foo = QueryBuilder
      .from[Person]
      .where(_.id === 5)
      .all
  }
}
