package usql.dao

import usql.util.TestBase

import scala.NamedTuple.NamedTuple

class DerivedColumnNameTest extends TestBase {

  case class Person(
      name: String,
      age: Int
  )

  it should "work" in {
    val x = (name = "Hello", age = 42)
    println(x.name)

    val names /*: NamedTuple[("hello", "world"), (String, String)]*/ = Macros.columnNameMapper[Person]
    // names.hello shouldBe "HELLO"

    // No autocomplete in IntelliJ, but in VS Code

    val p: Person = ???
    
  }
}
