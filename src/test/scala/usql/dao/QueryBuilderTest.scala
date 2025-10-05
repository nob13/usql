package usql.dao

import usql.util.TestBaseWithH2
import usql.DataType

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

  it should "work" in new EnvWithSamples {
    val withoutAgeQuery = Person.query
      .filter(_.age.isNull)
      .map(x => (x.id, x.name))

    println(s"SQL = ${withoutAgeQuery.sql}")

    val withoutAge = withoutAgeQuery.all()

    withoutAge should contain theSameElementsAs Seq(2 -> "Bob", 3 -> "Charly")
  }

  it should "with simple joins" in new EnvWithSamples {
    val foo = Person.query
      .join(PersonPermission.query)(_.id === _.personId)
      .join(Permission.query)(_._2.permissionId === _.id)
      .filter(_._2.name === "Write")
      .map(_._1._1.name)

    println(s"Foo SQL ${foo.sql}")
    foo.all() shouldBe Seq("Alice")

    val foo2 = Person.query
      .leftJoin(PersonPermission.query)(_.id === _.personId)
      .leftJoin(Permission.query)(_._2.permissionId === _.id)
      .map(x => (x._1._1.name, x._2.name))

    println(s"Foo2 SQL ${foo2.sql}")
    foo2.all() should contain theSameElementsAs Seq(
      ("Alice", Some("Read")),
      ("Alice", Some("Write")),
      ("Bob", Some("Read")),
      ("Charly", None)
    )
  }

  it should "work in a sub select case" in new EnvWithSamples {
    val persons = Person.query
      .map(p => (p.id, p.name))

    persons.all() shouldBe Seq(
      1 -> "Alice",
      2 -> "Bob",
      3 -> "Charly"
    )

    val permissions = Permission.query
      .join(QueryBuilder.make[PersonPermission])(_.id === _.permissionId)
      .map(x => (x._1.name, x._2.personId))

    val joinedAgain: QueryBuilder[((Int, String), (String, Int))] =
      persons.join(permissions)((person, permission) => person._1 === permission._2)
    val personAndPermission                                       = joinedAgain.map(x => (x._1._2, x._2._1))

    val expected = Seq(("Alice", "Read"), ("Alice", "Write"), ("Bob", "Read"))
    personAndPermission.all() should contain theSameElementsAs expected
  }

  it should "work with conflicting ids" in new EnvWithSamples {
    val persons = Person.query
      .map(p => (p.id, p.id))

    println(persons.all())
    persons.fielded.columns.distinct shouldBe Seq(
      SqlColumn("id", DataType.get[Int]),
      SqlColumn("id0", DataType.get[Int])
    )
  }
}
