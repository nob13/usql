package usql.dao

import usql.{DataType, RowDecoder, RowEncoder, SqlColumnId, SqlTableId}

import scala.annotation.Annotation
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}
import scala.reflect.ClassTag

object Macros {

  def getMaxOneAnnotation[T: ClassTag](in: List[Annotation]): Option[T] = {
    in.collect { case a: T =>
      a
    } match {
      case Nil       => None
      case List(one) => Some(one)
      case multiple  => throw new IllegalArgumentException(s"More than one annotation of same type found: ${multiple}")
    }
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
  case class TypeInfos[T](infos: List[TypeInfo[?]], builder: List[Any] => T)

  object TypeInfos {
    given forTuple[H, T <: Tuple](
        using typeInfo: TypeInfo[H],
        tailInfos: TypeInfos[T]
    ): TypeInfos[H *: T] = TypeInfos(
      typeInfo :: tailInfos.infos,
      builder = values => {
        values.head.asInstanceOf[H] *: tailInfos.builder(values.tail)
      }
    )
    given empty: TypeInfos[EmptyTuple] = TypeInfos(Nil, _ => EmptyTuple)
  }

  inline def buildTabular[T <: Product](using nm: NameMapping, mirror: Mirror.ProductOf[T]): SqlTabular[T] = {
    val fielded = buildFielded[T]

    val tableName: SqlTableId = tableNameAnnotation[T]
      .map { tn =>
        SqlTableId.fromString(tn.name)
      }
      .getOrElse {
        nm.caseClassToTableId(typeName[T])
      }

    SqlTabular.SimpleTabular(
      table = tableName,
      fielded = fielded,
      isOptional = false
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

  inline def annotationsExtractor[T]: List[List[Annotation]] = {
    ${ annotationsExtractorImpl[T] }
  }

  def annotationsExtractorImpl[T](using quotes: Quotes, t: Type[T]): Expr[List[List[Annotation]]] = {
    import quotes.reflect.*
    val tree   = TypeRepr.of[T]
    val symbol = tree.typeSymbol

    // Note: symbol.caseFields.map(_.annotations) does not work, but using the primaryConstructor works
    // Also see https://august.nagro.us/read-annotations-from-macro.html

    Expr.ofList(
      symbol.primaryConstructor.paramSymss.flatten
        .map { sym =>
          Expr.ofList {
            sym.annotations.collect {
              case term if (term.tpe <:< TypeRepr.of[Annotation]) =>
                term.asExprOf[Annotation]
            }
          }
        }
    )
  }

  inline def buildFielded[T <: Product](
      using nm: NameMapping,
      mirror: Mirror.ProductOf[T]
  ): SqlFielded[T] = {
    val labels: List[String]                = deriveLabels[T]
    val annotations: List[List[Annotation]] = annotationsExtractor[T]
    val typeInfos                           = summonInline[TypeInfos[mirror.MirroredElemTypes]]
    val splitter: T => List[Any]            = v => v.productIterator.toList

    val fields =
      labels.zip(annotations).zip(typeInfos.infos).map {
        case ((label, annotations), typeInfo: TypeInfo.Scalar[?]) =>
          val nameAnnotation = getMaxOneAnnotation[ColumnName](annotations)
          val id             = nameAnnotation.map(a => SqlColumnId.fromString(a.name)).getOrElse(nm.columnToSql(label))
          val column         = SqlColumn(id, typeInfo.dataType)
          Field.Column(label, column)
        case ((label, annotations), c: TypeInfo.Columnar[?])      =>
          val nameAnnotation = getMaxOneAnnotation[ColumnName](annotations)
          val columnGroup    = getMaxOneAnnotation[ColumnGroup](annotations)
          val mapping        = columnGroup.map(_.mapping).getOrElse(ColumnGroupMapping.Pattern())
          val columnBaseName =
            nameAnnotation.map(a => SqlColumnId.fromString(a.name)).getOrElse(nm.columnToSql(label))
          Field.Group(label, mapping, columnBaseName, c.columnar.asInstanceOf[SqlFielded[?]])
      }
    SqlFielded.SimpleSqlFielded(
      fields = fields,
      splitter = splitter,
      builder = typeInfos.builder.andThen(mirror.fromTuple)
    )
  }

}
