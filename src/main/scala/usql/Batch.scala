package usql

/** Encapsulates a batch (insert) */
case class Batch[T](sql: SqlBase, values: IterableOnce[T], encoder: RowEncoder[T]) {
  def run()(using cp: ConnectionProvider): Seq[Int] = {
    sql.withPreparedStatement { ps =>
      values.iterator.foreach { value =>
        encoder.encode(ps, value)
        ps.addBatch()
      }
      val results = ps.executeBatch()
      results.toSeq
    }
  }
}
