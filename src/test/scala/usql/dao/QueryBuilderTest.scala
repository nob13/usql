package usql.dao

import usql.util.TestBaseWithH2
import usql.profiles.PostgresProfile.*
import usql.sql

class QueryBuilderTest extends TestBaseWithH2 {
  override protected def baseSql: String =
    """
      |CREATE TABLE person (
      |  id INT PRIMARY KEY,
      |  name VARCHAR,
      |  age INT
      |);
      |""".stripMargin

  case class Person(
      id: Int,
      name: String,
      age: Option[Int] = None
  ) derives SqlTabular

  object Person extends KeyedCrudBase[Int, Person] {
    override def key: KeyColumnPath = cols.id

    override lazy val tabular: SqlTabular[Person] = summon
  }

  trait EnvWithSamples {
    val alice  = Person(1, "Alice", Some(42))
    val bob    = Person(2, "Bob", None)
    val charly = Person(3, "Charly", None)

    Person.insert(alice, bob, charly)
  }

  it should "make a simple query" in new EnvWithSamples {
    QueryBuilder
      .from[Person]
      .where(_.id === 1)
      .selectAll
      .one() shouldBe Some(alice)

    QueryBuilder
      .from[Person]
      .where(_.name === "Bob")
      .selectAll
      .all() shouldBe Seq(bob)

    val aliceAndBob = QueryBuilder
      .from[Person]
      .where(x => x.name === "Bob" || x.name === "Alice")
      .selectAll
      .all()
    aliceAndBob should contain theSameElementsAs Seq(alice, bob)

    val greater30 = QueryBuilder
      .from[Person]
      .where(_.age > 30)
      .selectAll
      .all()

    greater30 shouldBe Seq(alice)

    val withoutAge = QueryBuilder
      .from[Person]
      .where(_.age.isNull)
      .selectAll
      .all()

    withoutAge should contain theSameElementsAs Seq(bob, charly)
  }

  it should "select single fields" in new EnvWithSamples {
    val names = QueryBuilder.from[Person].select(_.name, _.age).all()
    names should contain theSameElementsAs Seq(("Alice", Some(42)), ("Bob", None), ("Charly", None))
  }

}
