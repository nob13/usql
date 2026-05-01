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
      |CREATE TABLE location (
      |  id INT PRIMARY KEY,
      |  person_id INT REFERENCES person(id),
      |  name VARCHAR,
      |  coordinate_x INT,
      |  coordinate_y INT,
      |  secondary_coordinate_x INT,
      |  secondary_coordinate_y INT
      |);
      |""".stripMargin

  case class Person(
      id: Int,
      name: String,
      age: Option[Int] = None
  )

  given personSqlTabular: SqlTabular[Person] = SqlTabular.derived

  object Person extends KeyedCrudBase[Int, Person] {
    override def key: KeyColumnPath = cols.id
  }

  case class Permission(
      id: Int,
      name: String
  )

  given permissionSqlTabular: SqlTabular[Permission] = SqlTabular.derived

  object Permission extends KeyedCrudBase[Int, Permission] {
    override def key: KeyColumnPath = cols.id
  }

  case class PersonPermission(
      personId: Int,
      permissionId: Int
  )

  given personPermissionSqlTabular: SqlTabular[PersonPermission] = SqlTabular.derived

  object PersonPermission extends CrdBase[PersonPermission]

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

    val withoutAge = withoutAgeQuery.all()

    withoutAge should contain theSameElementsAs Seq(2 -> "Bob", 3 -> "Charly")
    withoutAgeQuery.count() shouldBe 2
  }

  it should "correctly order operators" in new EnvWithSamples {
    val query = Person.query
      .filter(x => x.age.isNotNull && (x.name === "Alice" || x.name === "Bob"))
      .map(_.name)

    println(s"Query = ${query.sql}")

    val names = query.all()

    // This fails, when the query SELECT name AS name FROM person WHERE age IS NOT NULL AND name = ? OR name = ? is used

    names shouldBe Seq("Alice")
  }

  it should "also support negation operations" in new EnvWithSamples {
    val query = Person.query
      .filter(x => !x.age.isNull)
      .map(_.name)

    val names = query.all()
    names should contain theSameElementsAs Seq("Alice")
  }

  it should "work with a single join" in new EnvWithSamples {
    val foo: QueryBuilder[(Person, Int)] = Person.query
      .join(PersonPermission.query)(_.id === _.personId)
      .map(x => (x._1, x._2.permissionId))
    foo.all() should contain theSameElementsAs Seq(
      alice -> read.id,
      alice -> write.id,
      bob   -> read.id
    )

    foo.count() shouldBe 3
  }

  it should "support tuple-destructured map after a single join" in new EnvWithSamples {
    val foo: QueryBuilder[(Person, Int)] = Person.query
      .join(PersonPermission.query)(_.id === _.personId)
      .map((p, pp) => (p, pp.permissionId))
    foo.all() should contain theSameElementsAs Seq(
      alice -> read.id,
      alice -> write.id,
      bob   -> read.id
    )

    foo.count() shouldBe 3
  }

  it should "support tuple-destructured map after a left join" in new EnvWithSamples {
    val q = Person.query
      .leftJoin(PersonPermission.query)(_.id === _.personId)
      .leftJoin(Permission.query)(_._2.permissionId === _.id)
      .map((pp, perm) => (pp._1.name, perm.name))

    q.all() should contain theSameElementsAs Seq(
      ("Alice", Some("Read")),
      ("Alice", Some("Write")),
      ("Bob", Some("Read")),
      ("Charly", None)
    )
  }

  it should "with simple joins" in new EnvWithSamples {
    val foo = Person.query
      .join(PersonPermission.query)(_.id === _.personId)
      .join(Permission.query)(_._2.permissionId === _.id)
      .filter(_._2.name === "Write")
      .map(_._1._1.name)

    foo.all() shouldBe Seq("Alice")

    val foo2 = Person.query
      .leftJoin(PersonPermission.query)(_.id === _.personId)
      .leftJoin(Permission.query)(_._2.permissionId === _.id)
      .map(x => (x._1._1.name, x._2.name))

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

    persons.count() shouldBe 3

    val permissions = Permission.query
      .join(QueryBuilder.make[PersonPermission])(_.id === _.permissionId)
      .map(x => (x._1.name, x._2.personId))

    val joinedAgain: QueryBuilder[((Int, String), (String, Int))] =
      persons.join(permissions)((person, permission) => person._1 === permission._2)
    val personAndPermission                                       = joinedAgain.map(x => (x._1._2, x._2._1))

    val expected = Seq(("Alice", "Read"), ("Alice", "Write"), ("Bob", "Read"))
    personAndPermission.all() should contain theSameElementsAs expected
    personAndPermission.count() shouldBe 3
  }

  it should "work with conflicting ids" in new EnvWithSamples {
    val persons = Person.query
      .map(p => (p.id, p.id))

    persons.structure.columns.distinct shouldBe Seq(
      SqlColumn("id", DataType.get[Int]),
      SqlColumn("id1", DataType.get[Int])
    )
  }

  it should "delete in simple cases" in new EnvWithSamples {
    PersonPermission.query.delete()
    Permission.query.delete()
    Person.query.filter(_.id === 1).delete()
    val all = Person.findAll()
    all should contain theSameElementsAs Seq(bob, charly)
    Permission.findAll() shouldBe empty
    PersonPermission.findAll() shouldBe empty

    Person.query.count() shouldBe 2
    Permission.query.count() shouldBe 0
    PersonPermission.query.count() shouldBe 0
  }

  it should "update in simple cases" in new EnvWithSamples {
    Person.query.filter(_.id === 1).map(_.name).update("AliceX")
    val all = Person.findAll()
    all should contain theSameElementsAs Seq(alice.copy(name = "AliceX"), bob, charly)
  }

  case class Coordinate(x: Int, y: Int) derives SqlFielded

  case class Location(
      id: Int,
      personId: Int,
      name: String,
      @ColumnGroup coordinate: Coordinate,
      @ColumnGroup secondaryCoordinate: Option[Coordinate] = None
  )

  given locationSqlTabular: SqlTabular[Location] = SqlTabular.derived

  object Location extends KeyedCrudBase[Int, Location] {
    override def key: KeyColumnPath = cols.id
  }

  it should "work with joins on tables with embedded column groups" in new EnvWithSamples {
    val home = Location(1, alice.id, "Home", Coordinate(10, 20))
    val work = Location(2, bob.id, "Work", Coordinate(30, 40))
    Location.insert(home, work)

    val query = Person.query
      .join(Location.query)(_.id === _.personId)
      .map(x => (x._1.name, x._2))

    val results = query.all()
    results should contain theSameElementsAs Seq(
      ("Alice", home),
      ("Bob", work)
    )
  }

  it should "work with inner joins on tables with optional sub-fields" in new EnvWithSamples {
    val home = Location(1, alice.id, "Home", Coordinate(10, 20), Some(Coordinate(50, 60)))
    val work = Location(2, bob.id, "Work", Coordinate(30, 40), None)
    Location.insert(home, work)

    val query = Person.query
      .join(Location.query)(_.id === _.personId)
      .map(x => (x._1.name, x._2))

    val results = query.all()
    results should contain theSameElementsAs Seq(
      ("Alice", home),
      ("Bob", work)
    )
  }

  it should "work with left joins on tables with optional sub-fields" in new EnvWithSamples {
    val home = Location(1, alice.id, "Home", Coordinate(10, 20), Some(Coordinate(50, 60)))
    val work = Location(2, bob.id, "Work", Coordinate(30, 40), None)
    Location.insert(home, work)

    val query = Person.query
      .leftJoin(Location.query)(_.id === _.personId)
      .map(x => (x._1.name, x._2))

    val results = query.all()
    results should contain theSameElementsAs Seq(
      ("Alice", Some(home)),
      ("Bob", Some(work)),
      ("Charly", None)
    )
  }

  it should "support mapping to a NamedTuple with field-name access on results" in new EnvWithSamples {
    val q = Person.query.map(p => (name = p.name, age = p.age))

    val rows = q.all()
    rows should contain theSameElementsAs Seq(
      (name = "Alice", age = Some(42)),
      (name = "Bob", age = None),
      (name = "Charly", age = None)
    )

    val first = rows.find(_.name == "Alice").get
    first.name shouldBe "Alice"
    first.age shouldBe Some(42)
  }

  it should "preserve underlying column ids when mapping to a NamedTuple" in new EnvWithSamples {
    val q = Person.query.map(p => (renamedName = p.name, renamedAge = p.age))
    q.structure.columns.map(_.id.name) shouldBe Seq("name", "age")
  }

  it should "support filter by NamedTuple field name via .named" in new EnvWithSamples {
    val q       = Person.query.map(p => (name = p.name, age = p.age))
    val onlyBob = q.filter(_.named.name === "Bob").all()
    onlyBob shouldBe Seq((name = "Bob", age = None))
  }

  it should "support NamedTuple map after a join" in new EnvWithSamples {
    val q = Person.query
      .join(PersonPermission.query)(_.id === _.personId)
      .map((p, pp) => (person = p, permission = pp.permissionId))

    val rows = q.all()
    rows should contain theSameElementsAs Seq(
      (person = alice, permission = read.id),
      (person = alice, permission = write.id),
      (person = bob, permission = read.id)
    )

    val alicePerms = rows.filter(_.person == alice).map(_.permission)
    alicePerms should contain theSameElementsAs Seq(read.id, write.id)
  }

  it should "update whole objects" in new EnvWithSamples {
    val alice2 = alice.copy(
      name = "AliceX",
      age = Some(43)
    )
    Person.query.filter(_.id === 1).update(alice2) shouldBe 1
    Person.findAll() should contain theSameElementsAs Seq(alice2, bob, charly)

    val alice3 = alice2.copy(
      age = None
    )

    Person.query.filter(_.id === 1).update(alice3) shouldBe 1
    Person.findAll() should contain theSameElementsAs Seq(alice3, bob, charly)

    Person.query.filter(_.id === 666).update(alice) shouldBe 0
    Person.findAll() should contain theSameElementsAs Seq(alice3, bob, charly)
  }
}
