package usql

/** Returns underlying type of Option if T is an Option */
type UnOption[T] = T match {
  case Option[x] => x
  case _         => T
}

/** Returns the optional value, if not yet optional. */
type Optionalize[T] = T match {
  case Option[x] => T
  case _         => Option[T]
}

object Optionalize {
  
}