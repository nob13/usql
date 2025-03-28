package usql.dao

import usql.{DataType, ParameterFiller, ResultRowDecoder, SqlIdentifier, SqlIdentifiers}

import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

object Macros {
  inline def buildColumnar[T <: Product](
      using nm: NameMapping,
      mirror: Mirror.ProductOf[T]
  ): SqlColumnar.SimpleColumnar[T] = {
    val labels: List[String]                        = deriveLabels[T]
    val nameAnnotations: List[Option[ColumnName]]   = columnNameAnnotations[T]
    val groupAnnotations: List[Option[ColumnGroup]] = columnGroupAnnotations[T]
    val typeInfos                                   = summonInline[TypeInfos[mirror.MirroredElemTypes]]

    val colums = SqlColumns {
      labels.zip(nameAnnotations).zip(typeInfos.infos).zip(groupAnnotations).flatMap {
        case (((label, nameAnnotation), typeInfo: TypeInfo.Scalar[?]), _)                =>
          val id = nameAnnotation.map(a => SqlIdentifier.fromString(a.name)).getOrElse(nm.columnToSql(label))
          Some(
            SqlColumn(id, typeInfo.dataType)
          )
        case (((label, nameAnnotation), c: TypeInfo.Columnar[?]), maybeColumnAnnotation) =>
          val columnAnnotation = maybeColumnAnnotation.getOrElse(ColumnGroup())
          val memberName       = nameAnnotation.map(_.name).getOrElse(nm.columnToSql(label).name)
          c.columnar.columns.map { c =>
            val columnId = columnAnnotation.columnName(memberName, c.id)
            SqlColumn(columnId, c.dataType)
          }
      }
    }

    // RowDecoder/Parameter filler already handles nested columnar elements

    val rowDecoder      = summonInline[ResultRowDecoder[mirror.MirroredElemTypes]].map(mirror.fromTuple)
    val parameterFiller =
      summonInline[ParameterFiller[mirror.MirroredElemTypes]].contraMap[T](x => Tuple.fromProductTyped(x)(using mirror))

    SqlColumnar.SimpleColumnar(
      columns = colums,
      rowDecoder = rowDecoder,
      parameterFiller = parameterFiller
    )
  }

  /** Type info for each member, to differentiate between columnar and scalar types. */
  sealed trait TypeInfo[T]

  object TypeInfo {
    case class Scalar[T](dataType: DataType[T]) extends TypeInfo[T]

    case class Columnar[T](columnar: SqlColumnar[T]) extends TypeInfo[T]

    given scalar[T](using dt: DataType[T]): TypeInfo[T]     = Scalar(dt)
    given columnar[T](using c: SqlColumnar[T]): TypeInfo[T] = Columnar(c)
  }

  /** Combined TypeInfos for a tuple. */
  case class TypeInfos[T](infos: List[TypeInfo[_]])

  object TypeInfos {
    given forTuple[H, T <: Tuple](
        using typeInfo: TypeInfo[H],
        tailInfos: TypeInfos[T]
    ): TypeInfos[H *: T] = TypeInfos(typeInfo :: tailInfos.infos)
    given empty: TypeInfos[EmptyTuple] = TypeInfos(Nil)
  }

  inline def buildTabular[T <: Product](using nm: NameMapping, mirror: Mirror.ProductOf[T]): SqlTabular[T] = {
    val columnar = buildColumnar[T]

    val tableName: SqlIdentifier = tableNameAnnotation[T]
      .map { tn =>
        SqlIdentifier.fromString(tn.name)
      }
      .getOrElse {
        nm.caseClassToTableName(typeName[T])
      }

    SqlTabular.SimpleTabular(
      tableName = tableName,
      columnar.columns,
      columnar.rowDecoder,
      columnar.parameterFiller
    )
  }

  inline def typeName[T]: String = {
    ${ typeNameImpl[T] }
  }

  def typeNameImpl[T](using types: Type[T], quotes: Quotes): Expr[String] = {
    Expr(Type.show[T])
  }

  inline def deriveLabels[T](using m: Mirror.Of[T]): List[String] = {
    // Also See https://stackoverflow.com/a/70416544/335385
    summonLabels[m.MirroredElemLabels]
  }

  inline def summonLabels[T <: Tuple]: List[String] = {
    inline erasedValue[T] match {
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => summonInline[ValueOf[t]].value.asInstanceOf[String] :: summonLabels[ts]
    }
  }

  /** Extract table name annotation for the type. */
  inline def tableNameAnnotation[T]: Option[TableName] = {
    ${ tableNameAnnotationImpl[T] }
  }

  def tableNameAnnotationImpl[T](using quotes: Quotes, t: Type[T]): Expr[Option[TableName]] = {
    import quotes.reflect.*
    val tree   = TypeRepr.of[T]
    val symbol = tree.typeSymbol
    symbol.annotations.collectFirst {
      case term if (term.tpe <:< TypeRepr.of[TableName]) =>
        term.asExprOf[TableName]
    } match {
      case None    => '{ None }
      case Some(e) => '{ Some(${ e }) }
    }
  }

  /** Extract column name annotations for each column. */
  inline def columnNameAnnotations[T]: List[Option[ColumnName]] = {
    ${ fieldAnnotationExtractor[ColumnName, T] }
  }

  inline def columnGroupAnnotations[T]: List[Option[ColumnGroup]] = {
    ${ fieldAnnotationExtractor[ColumnGroup, T] }
  }

  def fieldAnnotationExtractor[A, T](using quotes: Quotes, t: Type[T], a: Type[A]): Expr[List[Option[A]]] = {
    import quotes.reflect.*
    val tree   = TypeRepr.of[T]
    val symbol = tree.typeSymbol

    // Note: symbol.caseFields.map(_.annotations) does not work, but using the primaryConstructor works
    // Also see https://august.nagro.us/read-annotations-from-macro.html

    Expr.ofList(
      symbol.primaryConstructor.paramSymss.flatten
        .map { sym =>
          sym.annotations.collectFirst {
            case term if (term.tpe <:< TypeRepr.of[A]) =>
              term.asExprOf[A]
          } match {
            case None    => '{ None }
            case Some(e) => '{ Some(${ e }) }
          }
        }
    )
  }

  inline def columnNameMapper[T](using m: Mirror.Of[T]) = {
    // NamedTuple.build[("hello", "world")]()("HELLO", "WORLD")
    // NamedTuple.build[("hello" *: EmptyTuple)]()(("HELLO", "BAR"))
    /*
    // Das geht nicht
    val labels                         = deriveLabels[T]
    def build(remaining: List[String]) = {
      remaining match {
        case Nil          => NamedTuple.Empty
        case head :: tail =>
          NamedTuple.build[(head)]((head.toUpperCase)) ++ build(tail)
      }
    }
    build(labels)

     */
    // columnNameMapperForTuple[m.MirroredElemLabels]
    ???
  }

  trait ColumnMapper2[T] {
    type Result

    def result: Result
  }

  object ColumnMapper2 {
    type Aux[T, R] = ColumnMapper2[T] {
      type Result = R
    }

    given empty: ColumnMapper2[EmptyTuple] with {
      override type Result = NamedTuple.Empty

      override def result: Result = NamedTuple.Empty
    }

    inline given forTuple[H, T <: Tuple](
        using value: ValueOf[H],
        tailMapper: ColumnMapper2[T]
    ): ColumnMapper2[H *: T] = {
      // ARGH NO
      // inline val string = value.value.asInstanceOf[String]
      // NamedTuple.build[(string)](Tuple1(string.toUpperCase)) ++ tailMapper.result
      ???
    }
  }

  inline def columnMapper2[T](using m: Mirror.ProductOf[T]) = {
    /*
    Vermutlich müssen wir das rekursiv machen:
    - Leeres Tuple -> Leeres Named Product
    - Rekursiver Schritt
     */
    ???
  }

  /*
  Das geht auch nicht so einfach, da der Type der Erstellung klar sein muss
  inline def columnNameMapperForTuple[T <: Tuple] = {
    inline erasedValue[T] match {
      case _: EmptyTuple => NamedTuple.Empty
      case _: (t *: ts)  =>
        NamedTuple[t](Tuple1(summonInline[ValueOf[t]].value.asInstanceOf[String])) ++ columnNameMapperForTuple[ts]
    }
  }
   */

}
