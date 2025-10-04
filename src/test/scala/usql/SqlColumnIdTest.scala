package usql

import usql.util.TestBase

class SqlColumnIdTest extends TestBase {
  "fromString" should "automatically quote" in {
    SqlColumnId.fromString("foo") shouldBe SqlColumnId("foo", false)
    SqlColumnId.fromString("id") shouldBe SqlColumnId("id", false)
    SqlColumnId.fromString("user") shouldBe SqlColumnId("user", true)
    SqlColumnId.fromString("\"foo\"") shouldBe SqlColumnId("foo", true)
    intercept[IllegalArgumentException] {
      SqlColumnId.fromString("\"id\"\"")
    }
  }
}
