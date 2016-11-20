package org.broadinstitute.hail.expr

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.broadinstitute.hail.utils._
import org.broadinstitute.hail.annotations.{Annotation, AnnotationPathException, _}
import org.broadinstitute.hail.check.Arbitrary._
import org.broadinstitute.hail.check.{Gen, _}
import org.broadinstitute.hail.utils
import org.broadinstitute.hail.utils.{Interval, StringEscapeUtils}
import org.broadinstitute.hail.variant.{AltAllele, Genotype, Locus, Variant}
import org.json4s._
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object Type {
  val genScalar = Gen.oneOf[Type](TBoolean, TChar, TInt, TLong, TFloat, TDouble, TString,
    TVariant, TAltAllele, TGenotype, TLocus, TInterval)

  def genSized(size: Int): Gen[Type] = {
    if (size < 1)
      Gen.const(TStruct.empty)
    else if (size < 2)
      genScalar
    else
      Gen.oneOfGen(genScalar,
        genArb.resize(size - 1).map(TArray),
        genArb.resize(size - 1).map(TSet),
        genArb.resize(size - 1).map(TDict),
        Gen.buildableOf[Array, (String, Type, Map[String, String])](
          Gen.zip(Gen.identifier,
            genArb,
            Gen.option(
              Gen.buildableOf2[Map, String, String](
                Gen.zip(arbitrary[String].filter(s => !s.isEmpty), arbitrary[String])))
              .map(o => o.getOrElse(Map.empty[String, String]))))
          .filter(fields => fields.map(_._1).areDistinct())
          .map(fields => TStruct(fields
            .iterator
            .zipWithIndex
            .map { case ((k, t, m), i) => Field(k, t, i, m) }
            .toIndexedSeq)))
  }


  def genArb: Gen[Type] = Gen.sized(genSized)

  implicit def arbType = Arbitrary(genArb)
}

sealed abstract class Type {

  def children: Seq[Type] = Seq()

  def clear(): Unit = children.foreach(_.clear())

  def unify(concrete: Type): Boolean = {
    println("Type.unify", this, concrete)
    this == concrete
  }

  def subst(): Type = this

  def getAsOption[T](fields: String*)(implicit ct: ClassTag[T]): Option[T] = {
    getOption(fields: _*)
      .flatMap { t =>
        if (ct.runtimeClass.isInstance(t))
          Some(t.asInstanceOf[T])
        else
          None
      }
  }

  def getOption(fields: String*): Option[Type] = getOption(fields.toList)

  def getOption(path: List[String]): Option[Type] = {
    if (path.isEmpty)
      Some(this)
    else
      None
  }

  def delete(fields: String*): (Type, Deleter) = delete(fields.toList)

  def delete(path: List[String]): (Type, Deleter) = {
    if (path.nonEmpty)
      throw new AnnotationPathException(s"invalid path ${ path.mkString(".") } from type ${ this }")
    else
      (TStruct.empty, a => Annotation.empty)
  }

  def insert(signature: Type, fields: String*): (Type, Inserter) = insert(signature, fields.toList)

  def insert(signature: Type, path: List[String]): (Type, Inserter) = {
    if (path.nonEmpty)
      TStruct.empty.insert(signature, path)
    else
      (signature, (a, toIns) => toIns.orNull)
  }

  def query(fields: String*): Querier = query(fields.toList)

  def query(path: List[String]): Querier = {
    if (path.nonEmpty)
      throw new AnnotationPathException(s"invalid path ${ path.mkString(".") } from type ${ this }")
    else
      a => Option(a)
  }

  def toPrettyString(compact: Boolean = false, printAttrs: Boolean = false): String = {
    val sb = new StringBuilder
    pretty(sb, compact = compact, printAttrs = printAttrs)
    sb.result()
  }

  def pretty(sb: StringBuilder, indent: Int = 0, printAttrs: Boolean = false, compact: Boolean = false) {
    sb.append(toString)
  }

  def fieldOption(fields: String*): Option[Field] = fieldOption(fields.toList)

  def fieldOption(path: List[String]): Option[Field] =
    None

  def schema: DataType = SparkAnnotationImpex.exportType(this)

  def str(a: Annotation): String = if (a == null) "NA" else a.toString

  def strVCF(a: Annotation): String = if (a == null) "." else a.toString

  def toJSON(a: Annotation): JValue = JSONAnnotationImpex.exportAnnotation(a, this)

  def genValue: Gen[Annotation] = Gen.const(Annotation.empty)

  def isRealizable: Boolean = children.forall(_.isRealizable)

  def typeCheck(a: Any): Boolean

  /* compare values for equality, but compare Float and Double values using D_== */
  def valuesSimilar(a1: Annotation, a2: Annotation, tolerance: Double = utils.defaultTolerance): Boolean = a1 == a2
}

case object TBinary extends Type {
  override def toString = "Binary"

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Array[Byte]]

  override def genValue: Gen[Annotation] = Gen.buildableOf(arbitrary[Byte])
}

case object TBoolean extends Type {
  override def toString = "Boolean"

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Boolean]

  def parse(s: String): Annotation = s.toBoolean

  override def genValue: Gen[Annotation] = arbitrary[Boolean]
}

case object TChar extends Type {
  override def toString = "Char"

  def typeCheck(a: Any): Boolean = a == null || (a.isInstanceOf[String]
    && a.asInstanceOf[String].length == 1)

  override def genValue: Gen[Annotation] = arbitrary[String]
    .filter(_.nonEmpty)
    .map(s => s.substring(0, 1))
}

object TNumeric {
  def promoteNumeric(types: Set[TNumeric]): Type = {
    if (types.size == 1)
      types.head
    else if (types(TDouble))
      TDouble
    else if (types(TFloat))
      TFloat
    else {
      assert(types(TLong))
      TLong
    }
  }
}

abstract class TNumeric extends Type {
  def conv: NumericConversion[_]
}

abstract class TIntegral extends TNumeric

case object TInt extends TIntegral {
  override def toString = "Int"

  val conv = IntNumericConversion

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Int]

  override def genValue: Gen[Annotation] = arbitrary[Int]
}

case object TLong extends TIntegral {
  override def toString = "Long"

  val conv = LongNumericConversion

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Long]

  override def genValue: Gen[Annotation] = arbitrary[Long]
}

case object TFloat extends TNumeric {
  override def toString = "Float"

  val conv = FloatNumericConversion

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Float]

  override def str(a: Annotation): String = if (a == null) "NA" else a.asInstanceOf[Float].formatted("%.5e")

  override def genValue: Gen[Annotation] = arbitrary[Double].map(_.toFloat)

  override def valuesSimilar(a1: Annotation, a2: Annotation, tolerance: Double): Boolean =
    a1 == a2 || (a1 != null && a2 != null && D_==(a1.asInstanceOf[Float], a2.asInstanceOf[Float], tolerance))
}

case object TDouble extends TNumeric {
  override def toString = "Double"

  val conv = DoubleNumericConversion

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Double]

  override def str(a: Annotation): String = if (a == null) "NA" else a.asInstanceOf[Double].formatted("%.5e")

  override def genValue: Gen[Annotation] = arbitrary[Double]

  override def valuesSimilar(a1: Annotation, a2: Annotation, tolerance: Double): Boolean =
    a1 == a2 || (a1 != null && a2 != null && D_==(a1.asInstanceOf[Double], a2.asInstanceOf[Double], tolerance))
}

case object TString extends Type {
  override def toString = "String"

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[String]

  override def genValue: Gen[Annotation] = arbitrary[String]
}

case class TFunction(paramTypes: Seq[Type], returnType: Type) extends Type {
  override def isRealizable = false

  override def unify(concrete: Type) = {
    concrete match {
      case TFunction(cparamTypes, creturnType) =>
        paramTypes.length == cparamTypes.length &&
          (paramTypes, cparamTypes).zipped.forall { case (pt, cpt) =>
              pt.unify(cpt)
          } &&
        returnType.unify(creturnType)

      case _ => false
    }
  }

  override def subst() = TFunction(paramTypes.map(_.subst()), returnType.subst())

  def typeCheck(a: Any) = ???

  override def children = paramTypes :+ returnType
}

case class TVariable(var t: Type = null) extends Type {
  override def isRealizable = false

  def typeCheck(a: Any) = ???

  override def unify(concrete: Type) = {
    if (t == null) {
      t = concrete
      true
    } else
      t == concrete
  }

  override def clear() {
    t = null
  }

  override def subst() = {
    assert(t != null)
    t
  }
}

abstract class TAggregable extends Type {
  override def isRealizable = false

  def typeCheck(a: Any) = ???

  override def toString: String = s"Aggregable[${ elementType.toString }]"

  def ec: EvalContext

  def elementType: Type

  def f: (Any) => Any
}

case class BaseAggregable(ec: EvalContext, elementType: Type) extends TAggregable {
  def f: (Any) => Any = identity
}

case class FilteredAggregable(parent: TAggregable, filterF: (Any) => Boolean) extends TAggregable {
  def f: (Any) => Any = {
    val parentF = parent.f
    (a: Any) => {
      val prev = parentF(a)
      if (prev != null && filterF(prev))
        prev
      else
        null
    }
  }

  override def elementType: Type = parent.elementType

  override def ec: EvalContext = parent.ec
}

case class MappedAggregable(parent: TAggregable, elementType: Type, mapF: (Any) => Any) extends TAggregable {
  def f: (Any) => Any = {
    val parentF = parent.f
    (a: Any) => {
      val prev = parentF(a)
      if (prev != null)
        mapF(prev)
      else
        null
    }
  }

  override def ec: EvalContext = parent.ec
}

abstract class TContainer extends Type {
  def elementType: Type

  override def children = Seq(elementType)
}

abstract class TIterable extends TContainer {
  override def valuesSimilar(a1: Annotation, a2: Annotation, tolerance: Double): Boolean =
    a1 == a2 || (a1 != null && a2 != null
      && (a1.asInstanceOf[Iterable[_]].size == a2.asInstanceOf[Iterable[_]].size)
      && a1.asInstanceOf[Iterable[_]].zip(a2.asInstanceOf[Iterable[_]])
      .forall { case (e1, e2) => elementType.valuesSimilar(e1, e2, tolerance) })
}

case class TArray(elementType: Type) extends TIterable {
  override def toString = s"Array[$elementType]"

  override def unify(concrete: Type) = {
    println("TArray.unify", this, concrete)
    concrete match {
      case TArray(celementType) => elementType.unify(celementType)
      case _ => false
    }
  }

  override def subst() = TArray(elementType.subst())

  override def pretty(sb: StringBuilder, indent: Int, printAttrs: Boolean, compact: Boolean = false) {
    sb.append("Array[")
    elementType.pretty(sb, indent, printAttrs, compact)
    sb.append("]")
  }

  def typeCheck(a: Any): Boolean = a == null || (a.isInstanceOf[IndexedSeq[_]] &&
    a.asInstanceOf[IndexedSeq[_]].forall(elementType.typeCheck))

  override def str(a: Annotation): String = JsonMethods.compact(toJSON(a))

  override def genValue: Gen[Annotation] = Gen.buildableOf[IndexedSeq, Annotation](elementType.genValue)
}

case class TSet(elementType: Type) extends TIterable {
  override def toString = s"Set[$elementType]"

  override def unify(concrete: Type) = concrete match {
    case TSet(celementType) => elementType.unify(celementType)
    case _ => false
  }

  override def subst() = TSet(elementType.subst())

  def typeCheck(a: Any): Boolean =
    a == null || (a.isInstanceOf[Set[_]] && a.asInstanceOf[Set[_]].forall(elementType.typeCheck))

  override def pretty(sb: StringBuilder, indent: Int, printAttrs: Boolean, compact: Boolean = false) {
    sb.append("Set[")
    elementType.pretty(sb, indent, printAttrs, compact)
    sb.append("]")
  }

  override def str(a: Annotation): String = JsonMethods.compact(toJSON(a))

  override def genValue: Gen[Annotation] = Gen.buildableOf[Set, Annotation](elementType.genValue)
}

case class TDict(elementType: Type) extends TContainer {
  override def children = Seq(elementType)

  override def unify(concrete: Type) = {
    println("TDict.unify", this, concrete)
    concrete match {
      case TDict(celementType) => elementType.unify(celementType)
      case _ => false
    }
  }

  override def subst() = TDict(elementType.subst())

  override def toString = s"Dict[$elementType]"

  override def pretty(sb: StringBuilder, indent: Int, printAttrs: Boolean, compact: Boolean = false) {
    sb.append("Dict[")
    elementType.pretty(sb, indent, printAttrs, compact)
    sb.append("]")
  }

  def typeCheck(a: Any): Boolean = a == null || (a.isInstanceOf[Map[_, _]] &&
    a.asInstanceOf[Map[_, _]].forall { case (k, v) => k.isInstanceOf[String] && elementType.typeCheck(v) })

  override def str(a: Annotation): String = JsonMethods.compact(toJSON(a))

  override def genValue: Gen[Annotation] =
    Gen.buildableOf2[Map, String, Annotation](Gen.zip(arbitrary[String], elementType.genValue))

  override def valuesSimilar(a1: Annotation, a2: Annotation, tolerance: Double): Boolean =
    a1 == a2 || (a1 != null && a2 != null) ||
      a1.asInstanceOf[Map[String, _]].outerJoin(a2.asInstanceOf[Map[String, _]])
        .forall { case (_, (o1, o2)) =>
          o1.liftedZip(o2).exists { case (v1, v2) => elementType.valuesSimilar(v1, v2, tolerance) }
        }
}

case object TSample extends Type {
  override def toString = "Sample"

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[String]

  override def genValue: Gen[Annotation] = Gen.identifier
}

case object TGenotype extends Type {
  override def toString = "Genotype"

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Genotype]

  override def genValue: Gen[Annotation] = Genotype.genArb
}

case object TAltAllele extends Type {
  override def toString = "AltAllele"

  def typeCheck(a: Any): Boolean = a == null || a == null || a.isInstanceOf[AltAllele]

  override def genValue: Gen[Annotation] = AltAllele.gen
}

case object TVariant extends Type {
  override def toString = "Variant"

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Variant]

  override def genValue: Gen[Annotation] = Variant.gen
}

case object TLocus extends Type {
  override def toString = "Locus"

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Locus]

  override def genValue: Gen[Annotation] = Locus.gen
}

case object TInterval extends Type {
  override def toString = "Interval"

  def typeCheck(a: Any): Boolean = a == null || a.isInstanceOf[Interval[_]] && a.asInstanceOf[Interval[_]].end.isInstanceOf[Locus]

  override def genValue: Gen[Annotation] = Interval.gen(Locus.gen)
}

case class Field(name: String, `type`: Type,
  index: Int,
  attrs: Map[String, String] = Map.empty) {
  def attr(s: String): Option[String] = attrs.get(s)

  def unify(cf: Field): Boolean =
    name == cf.name &&
      `type`.unify(cf.`type`) &&
      index == cf.index &&
      attrs == cf.attrs

  def pretty(sb: StringBuilder, indent: Int, printAttrs: Boolean, compact: Boolean) {
    if (compact) {
      sb.append(prettyIdentifier(name))
      sb.append(":")
    } else {
      sb.append(" " * indent)
      sb.append(prettyIdentifier(name))
      sb.append(": ")
    }
    `type`.pretty(sb, indent, printAttrs, compact)
    if (printAttrs) {
      attrs.foreach { case (k, v) =>
        if (!compact) {
          sb += '\n'
          sb.append(" " * (indent + 2))
        }
        sb += '@'
        sb.append(prettyIdentifier(k))
        sb.append("=\"")
        sb.append(StringEscapeUtils.escapeString(v))
        sb += '"'
      }
    }
  }
}

case class TSplat(struct: TStruct) extends Type {
  override def isRealizable = false

  def typeCheck(a: Any) = ???

  override def children = struct.children

  override def toString: String = "Splat"
}

object TStruct {
  def empty: TStruct = TStruct(Array.empty[Field])

  def apply(args: (String, Type)*): TStruct =
    TStruct(args
      .iterator
      .zipWithIndex
      .map { case ((n, t), i) => Field(n, t, i) }
      .toArray)
}

case class TStruct(fields: IndexedSeq[Field]) extends Type {
  override def children = fields.map(_.`type`)

  override def unify(concrete: Type) = concrete match {
    case TStruct(cfields) =>
      fields.length == cfields.length &&
        (fields, cfields).zipped.forall { case (f, cf) =>
          f.unify(cf)
        }
    case _ => false
  }

  override def subst() = TStruct(fields.map(f => f.copy(`type` = f.`type`.subst().asInstanceOf[Type])))

  val fieldIdx: Map[String, Int] =
    fields.map(f => (f.name, f.index)).toMap

  def selfField(name: String): Option[Field] = fieldIdx.get(name).map(i => fields(i))

  def size: Int = fields.length

  override def getOption(path: List[String]): Option[Type] =
    if (path.isEmpty)
      Some(this)
    else
      selfField(path.head).map(_.`type`).flatMap(t => t.getOption(path.tail))

  override def fieldOption(path: List[String]): Option[Field] =
    if (path.isEmpty)
      None
    else {
      val f = selfField(path.head)
      if (path.length == 1)
        f
      else
        f.flatMap(_.`type`.fieldOption(path.tail))
    }

  override def query(p: List[String]): Querier = {
    if (p.isEmpty)
      a => Option(a)
    else {
      selfField(p.head) match {
        case Some(f) =>
          val q = f.`type`.query(p.tail)
          val localIndex = f.index
          a =>
            if (a == Annotation.empty)
              None
            else
              q(a.asInstanceOf[Row].get(localIndex))
        case None => throw new AnnotationPathException(s"struct has no field ${ p.head }")
      }
    }
  }

  override def delete(p: List[String]): (Type, Deleter) = {
    if (p.isEmpty)
      (TStruct.empty, a => Annotation.empty)
    else {
      val key = p.head
      val f = selfField(key) match {
        case Some(f) => f
        case None => throw new AnnotationPathException(s"$key not found")
      }
      val index = f.index
      val (newFieldType, d) = f.`type`.delete(p.tail)
      val newType: Type =
        if (newFieldType == TStruct.empty)
          deleteKey(key, f.index)
        else
          updateKey(key, f.index, newFieldType)

      val localDeleteFromRow = newFieldType == TStruct.empty

      val deleter: Deleter = { a =>
        if (a == Annotation.empty)
          Annotation.empty
        else {
          val r = a.asInstanceOf[Row]

          if (localDeleteFromRow)
            r.delete(index)
          else
            r.update(index, d(r.get(index)))
        }
      }
      (newType, deleter)
    }
  }

  override def insert(signature: Type, p: List[String]): (Type, Inserter) = {
    if (p.isEmpty)
      (signature, (a, toIns) => toIns.orNull)
    else {
      val key = p.head
      val f = selfField(key)
      val keyIndex = f.map(_.index)
      val (newKeyType, keyF) = f
        .map(_.`type`)
        .getOrElse(TStruct.empty)
        .insert(signature, p.tail)

      val newSignature = keyIndex match {
        case Some(i) => updateKey(key, i, newKeyType)
        case None => appendKey(key, newKeyType)
      }

      val localSize = fields.size

      val inserter: Inserter = (a, toIns) => {
        val r = if (a == null || localSize == 0) // localsize == 0 catches cases where we overwrite a path
          Row.fromSeq(Array.fill[Any](localSize)(null))
        else
          a.asInstanceOf[Row]
        keyIndex match {
          case Some(i) => r.update(i, keyF(r.get(i), toIns))
          case None => r.append(keyF(Annotation.empty, toIns))
        }
      }
      (newSignature, inserter)
    }
  }

  def updateKey(key: String, i: Int, sig: Type): Type = {
    assert(fieldIdx.contains(key))

    val newFields = Array.fill[Field](fields.length)(null)
    for (i <- fields.indices)
      newFields(i) = fields(i)
    newFields(i) = Field(key, sig, i)
    TStruct(newFields)
  }

  def deleteKey(key: String, index: Int): Type = {
    assert(fieldIdx.contains(key))
    if (fields.length == 1)
      TStruct.empty
    else {
      val newFields = Array.fill[Field](fields.length - 1)(null)
      for (i <- 0 until index)
        newFields(i) = fields(i)
      for (i <- index + 1 until fields.length)
        newFields(i - 1) = fields(i).copy(index = i - 1)
      TStruct(newFields)
    }
  }

  def appendKey(key: String, sig: Type): TStruct = {
    assert(!fieldIdx.contains(key))
    val newFields = Array.fill[Field](fields.length + 1)(null)
    for (i <- fields.indices)
      newFields(i) = fields(i)
    newFields(fields.length) = Field(key, sig, fields.length)
    TStruct(newFields)
  }

  def merge(other: TStruct): (TStruct, Merger) = {

    val intersect = fields.map(_.name).toSet
      .intersect(other.fields.map(_.name).toSet)

    if (intersect.nonEmpty)
      fatal(
        s"""Invalid merge operation: cannot merge structs with same-name ${ plural(intersect.size, "field") }
            |  Found these fields in both structs: [ ${
          intersect.map(s => prettyIdentifier(s)).mkString(", ")
        } ]
            |  Hint: use `drop' or `select' to remove these fields from one side""".stripMargin)

    val newStruct = TStruct(fields ++ other.fields.map(f => f.copy(index = f.index + size)))

    val size1 = size
    val size2 = other.size
    val targetSize = newStruct.size

    val merger = (a1: Annotation, a2: Annotation) => {
      if (a1 == null && a2 == null)
        Annotation.empty
      else {
        val s1 = Option(a1).map(_.asInstanceOf[Row].toSeq)
          .getOrElse(Seq.fill[Any](size1)(null))
        val s2 = Option(a2).map(_.asInstanceOf[Row].toSeq)
          .getOrElse(Seq.fill[Any](size2)(null))
        val newValues = s1 ++ s2
        assert(newValues.size == targetSize)
        Annotation.fromSeq(newValues)
      }
    }

    (newStruct, merger)
  }

  def filter(set: Set[String], include: Boolean = true): (TStruct, Deleter) = {
    val notFound = set.filter(name => selfField(name).isEmpty).map(prettyIdentifier)
    if (notFound.nonEmpty)
      fatal(
        s"""invalid struct filter operation: ${
          plural(notFound.size, s"field ${ notFound.head }", s"fields [ ${ notFound.mkString(", ") } ]")
        } not found
            |  Existing struct fields: [ ${ fields.map(f => prettyIdentifier(f.name)).mkString(", ") } ]""".stripMargin)

    val fn = (f: Field) =>
      if (include)
        set.contains(f.name)
      else
        !set.contains(f.name)
    filter(fn)
  }

  def parseInStructScope[T](code: String, expected: Type): (Annotation) => Option[T] = {
    val ec = EvalContext(fields.map(f => (f.name, f.`type`)): _*)
    val f = Parser.parse[T](code, ec, expected)

    (a: Annotation) => {
      Option(a).flatMap { annotation =>
        ec.setAll(annotation.asInstanceOf[Row].toSeq: _*)
        f()
      }
    }
  }

  def filter(f: (Field) => Boolean): (TStruct, Deleter) = {
    val included = fields.map(f)

    val newFields = fields.zip(included)
      .flatMap { case (field, incl) =>
        if (incl)
          Some(field.name -> field.`type`)
        else
          None
      }

    val newSize = newFields.size

    val filterer = (a: Annotation) =>
      if (a == null)
        a
      else if (newSize == 0)
        Annotation.empty
      else {
        val r = a.asInstanceOf[Row]
        val newValues = included.zipWithIndex
          .flatMap { case (incl, i) =>
            if (incl)
              Some(r.get(i))
            else None
          }
        assert(newValues.length == newSize)
        Annotation.fromSeq(newValues)
      }

    (TStruct(newFields: _*), filterer)
  }

  override def toString = if (size == 0) "Empty" else "Struct"

  override def pretty(sb: StringBuilder, indent: Int, printAttrs: Boolean, compact: Boolean) {
    if (size == 0)
      sb.append("Empty")
    else {
      if (compact) {
        sb.append("Struct{")
        fields.foreachBetween(_.pretty(sb, indent, printAttrs, compact))(sb += ',')
        sb += '}'
      } else {
        sb.append("Struct {")
        sb += '\n'
        fields.foreachBetween(_.pretty(sb, indent + 4, printAttrs, compact))(sb.append(",\n"))
        sb += '\n'
        sb.append(" " * indent)
        sb += '}'
      }
    }
  }

  override def typeCheck(a: Any): Boolean =
    if (fields.isEmpty)
      a == null
    else a == null ||
      a.isInstanceOf[Row] && {
        val r = a.asInstanceOf[Row]
        r.length == fields.length &&
          r.toSeq.zip(fields).forall { case (v, f) => f.`type`.typeCheck(v) }
      }

  override def str(a: Annotation): String = JsonMethods.compact(toJSON(a))

  override def genValue: Gen[Annotation] = {
    if (size == 0)
      Gen.const(Annotation.empty)
    else
      Gen.size.flatMap(fuel =>
        if (size < fuel) Gen.const(Annotation.empty)
        else Gen.uniformSequence(fields.map(f => f.`type`.genValue)).map(a => Annotation(a: _*)))
  }

  override def valuesSimilar(a1: Annotation, a2: Annotation, tolerance: Double): Boolean =
    a1 == a2 || (a1 != null && a2 != null
      && fields.zip(a1.asInstanceOf[Row].toSeq).zip(a2.asInstanceOf[Row].toSeq)
      .forall { case ((f, x1), x2) =>
        f.`type`.valuesSimilar(x1, x2, tolerance)
      })
}
