package usql.profiles

import usql.dao.SqlFielded
import usql.*
import usql.util.TestBaseWithH2

import java.sql.{JDBCType, PreparedStatement, ResultSet}

class H2RowTypeTest extends TestBaseWithH2 {

  case class Data(
      foo: Option[Int],
      bar: Option[String]
  )

  // Not implicit or given, so that we do not interfer with embedded types
  val dataFielded: SqlFielded[Data] = SqlFielded.derived

  given dataDataType: DataType[Data] with {
    override def jdbcType: JDBCType = ???

    override def extractBySqlIdx(cIdx: Int, rs: ResultSet): Data = {
      (for
        rawObject <- Option(rs.getObject(cIdx))
        casted     = rawObject.asInstanceOf[ResultSet]
        if casted.next()
      yield {
        dataFielded.rowDecoder.parseRow(casted)
      }).getOrElse {
        Data(None, None)
      }
    }

    override def fillBySqlIdx(pIdx: Int, ps: PreparedStatement, value: Data): Unit = ???
  }

  case class Demo(id: Int, data: Data) derives SqlFielded

  it should "run with ROW types" in {
    runSql(
      """
        |CREATE TABLE demo (
        |        id BIGINT PRIMARY KEY,
        |        data ROW(foo INT, bar VARCHAR)
        |    );
        |
        |    INSERT INTO demo (id, data) VALUES
        |    (1, ROW(42, 'Hello')),
        |    (2, ROW(43, 'World'))
        |""".stripMargin
    )

    val result = sql"SELECT * FROM demo".query.all[Demo]()
    println(s"Result: ${result}")
  }

}
