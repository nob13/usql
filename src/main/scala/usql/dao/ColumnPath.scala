package usql.dao

case class ColumnPath[T](fields: List[String]) extends Selectable {
  type Fields = NamedTuple.Map[NamedTuple.From[T], ColumnPath]

  def selectDynamic(name: String): ColumnPath[?] = {
    ColumnPath(name :: fields)
  }

  def show: String = fields.mkString(",")
}
