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
      |CREATE TABLE permission(
      |  id INT PRIMARY KEY,
      |  name VARCHAR
      |);
      |CREATE TABLE person_permission(
      |  person_id INT REFERENCES person(id),
      |  permission_id INT REFERENCES permission(id),
      |  PRIMARY KEY(person_id, permission_id)
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

  case class Permission(
      id: Int,
      name: String
  ) derives SqlTabular

  object Permission extends KeyedCrudBase[Int, Permission] {
    override def key: KeyColumnPath = cols.id

    override lazy val tabular: SqlTabular[Permission] = summon
  }

  case class PersonPermission(
      personId: Int,
      permissionId: Int
  ) derives SqlTabular

  object PersonPermission extends CrdBase[PersonPermission] {
    override lazy val tabular: SqlTabular[PersonPermission] = summon
  }

  trait EnvWithSamples {
    val alice  = Person(1, "Alice", Some(42))
    val bob    = Person(2, "Bob", None)
    val charly = Person(3, "Charly", None)

    Person.insert(alice, bob, charly)

    val read  = Permission(1, "Read")
    val write = Permission(2, "Write")

    Permission.insert(read, write)

    PersonPermission.insert(
      PersonPermission(alice.id, read.id),
      PersonPermission(alice.id, write.id),
      PersonPermission(bob.id, read.id)
    )
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

  it should "work for a simple join" in new EnvWithSamples {
    val foo = QueryBuilder
      .fromX[Person]
      .join[PersonPermission](_.id == _.personId)

    val foo2 = foo
      .join[Permission](_._2.permissionId == _.id)

  }

  it should "work with Query2" in new EnvWithSamples {
    val withoutAgeQuery = Query2
      .make[Person]
      .filter(_.age.isNull)
      .map(x => (x.id, x.name))

    println(s"SQL = ${withoutAgeQuery.toSql}")

    val withoutAge = withoutAgeQuery.all()

    withoutAge should contain theSameElementsAs Seq(2 -> "Bob", 3 -> "Charly")
  }

  it should "join with Query2" in new EnvWithSamples {
    val foo = Query2
      .make[Person]
      .join(Query2.make[PersonPermission])(_.id === _.personId)
      .join(Query2.make[Permission])(_._2.permissionId === _.id)
      .filter(_._2.name === "Write")
      .map(_._1._1.name)

    println(s"Foo SQL ${foo.toSql}")
    foo.all() shouldBe Seq("Alice")

    val foo2 = Query2
      .make[Person]
      .leftJoin(Query2.make[PersonPermission])(_.id === _.personId)
      .leftJoin(Query2.make[Permission])(_._2.permissionId === _.id)
      .map(x => (x._1._1.name, x._2.name))

    println(s"Foo2 SQL ${foo2.toSql}")
    foo2.all() should contain theSameElementsAs Seq(
      ("Alice", Some("Read")),
      ("Alice", Some("Write")),
      ("Bob", Some("Read")),
      ("Charly", None)
    )
  }

  it should "work with conflicting column names" in {
    pending
  }
}
