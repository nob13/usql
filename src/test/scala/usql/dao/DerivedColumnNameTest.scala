package usql.dao

import usql.util.TestBase
import usql.profiles.PostgresProfile.*

class DerivedColumnNameTest extends TestBase {

  case class Coordinate(
      x: Int,
      y: Int
  ) derives SqlTabular

  case class Person(
      name: String,
      age: Int,
      coordinate: Coordinate
  ) derives SqlTabular

  object Person extends CrdBase[Person] {
    override lazy val tabular: SqlTabular[Person] = summon
  }

  it should "work" in {
    val x = (name = "Hello", age = 42)
    println(x.name)

    // val names /*: NamedTuple[("hello", "world"), (String, String)]*/ = Macros.columnNameMapper[Person]
    // names.hello shouldBe "HELLO"

    // No autocomplete in IntelliJ, but in VS Code

    println(s"Path1: ${Person.col.name.show}")
    println(s"Path2: ${Person.col.coordinate.x.show}")

  }
}
