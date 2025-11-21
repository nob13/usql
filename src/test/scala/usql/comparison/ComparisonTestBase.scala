package usql.comparison

import usql.util.TestBase

abstract class ComparisonTestBase extends TestBase {
  protected def lineTrim(s: String): String = {
    s.linesIterator.map(_.trim).mkString("\n")
  }
}
