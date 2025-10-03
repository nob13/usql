package usql.dao

import usql.SqlIdentifier

private[usql] case class ColumnPathOptImpl[R, T](underlying: ColumnPath[R, Option[T]]) extends ColumnPathOpt[R, T] {
  override def selectDynamic(name: String): ColumnPathOpt[R, _] = {
    val child = underlying.selectDynamic(name)
    ColumnPathOptImpl(
      child.asInstanceOf[ColumnPath[R, Option[?]]]
    )
  }

  override def buildGetter: R => Option[T] = {
    underlying.buildGetter
  }

  override def structure: SqlFielded[Option[T]] | SqlColumn[Option[T]] = {
    underlying.structure
  }

  override def withAlias(alias: String): ColumnPathOpt[R, T] = copy(
    underlying.withAlias(alias)
  )

  override def buildIdentifier: Seq[SqlIdentifier] = underlying.buildIdentifier

  override def toString: String = {
    underlying.toString
  }
}
