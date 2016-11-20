package org.broadinstitute.hail.expr

import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.stats._
import org.broadinstitute.hail.utils._
import org.broadinstitute.hail.variant.{AltAllele, Genotype, Locus, Variant}
import org.broadinstitute.hail.expr.HailRep._

import scala.collection.mutable
import org.broadinstitute.hail.utils.EitherIsAMonad._

object FunctionRegistry {

  sealed trait LookupError {
    def message: String
  }

  sealed case class NotFound(name: String, typ: TypeTag) extends LookupError {
    def message = s"No function found with name `$name' and argument ${ plural(typ.xs.size, "type") } $typ"
  }

  sealed case class Ambiguous(name: String, typ: TypeTag, alternates: Seq[(Int, (TypeTag, Fun))]) extends LookupError {
    def message =
      s"""found ${ alternates.size } ambiguous matches for $typ:
          |  ${ alternates.map(_._2._1).mkString("\n  ") }""".stripMargin
  }

  type Err[T] = Either[LookupError, T]

  private val registry = mutable.HashMap[String, Seq[(TypeTag, Fun)]]().withDefaultValue(Seq.empty)

  private val conversions = new mutable.HashMap[(Type, Type), (Int, UnaryFun[Any, Any])]

  private def lookupConversion(from: Type, to: Type): Option[(Int, UnaryFun[Any, Any])] = conversions.get(from -> to)

  private def registerConversion[T, U](how: T => U, priority: Int = 1)(implicit hrt: HailRep[T], hru: HailRep[U]) {
    val from = hrt.typ
    val to = hru.typ
    require(priority >= 1)
    lookupConversion(from, to) match {
      case Some(_) =>
        throw new RuntimeException(s"The conversion between $from and $to is already bound")
      case None =>
        conversions.put(from -> to, priority -> UnaryFun[Any, Any](to, x => how(x.asInstanceOf[T])))
    }
  }

  private def lookup(name: String, typ: TypeTag): Err[Fun] = {

    val matches = registry(name).flatMap { case (tt, f) =>
      tt.clear()
      if (tt.unify(typ)) {
        Some(0 -> (tt.subst(), f.subst()))
      } else if (tt.xs.size == typ.xs.size) {
        val conversionPriorities: Seq[Option[(Int, UnaryFun[Any, Any])]] = typ.xs.zip(tt.xs)
          .map { case (l, r) =>
            if (l == r)
              Some(0 -> UnaryFun[Any, Any](l, (a: Any) => a))
            else lookupConversion(l, r)
          }

        anyFailAllFail[Array, (Int, UnaryFun[Any, Any])](conversionPriorities).map(arr =>
          arr.map(_._1).max -> (tt, f.convertArgs(arr.map(_._2))))
      }
      else
        None
    }.groupBy(_._1).toArray.sortBy(_._1)

    matches.headOption
      .toRight[LookupError](NotFound(name, typ))
      .flatMap { case (priority, it) =>
        assert(it.nonEmpty)
        if (it.size == 1)
          Right(it.head._2._2)
        else {
          assert(priority != 0)
          Left(Ambiguous(name, typ, it))
        }
      }
  }

  private def bind(name: String, typ: TypeTag, f: Fun) = {
    lookup(name, typ) match {
      case Right(existingBinding) =>
        throw new RuntimeException(s"The name, $name, with type, $typ, is already bound as $existingBinding")
      case _ =>
        registry.updateValue(name, Seq.empty, (typ, f) +: _)
    }
  }

  def lookupMethodReturnType(typ: Type, typs: Seq[Type], name: String): Err[Type] =
    lookup(name, MethodType(typ +: typs: _*)).map(_.retType)

  def lookupMethod(ec: EvalContext)(typ: Type, typs: Seq[Type], name: String)(lhs: AST, args: Seq[AST]): Err[() => Any] = {
    require(typs.length == args.length)

    val m = lookup(name, MethodType(typ +: typs: _*))
    m.map {
      case f: UnaryFun[_, _] =>
        AST.evalCompose(ec, lhs)(f)
      case f: OptionUnaryFun[_, _] =>
        AST.evalFlatCompose(ec, lhs)(f)
      case f: BinaryFun[_, _, _] =>
        AST.evalCompose(ec, lhs, args(0))(f)
      case f: BinaryLambdaFun[t, _, _] =>
        val Lambda(_, param, body) = args(0)

        val localIdx = ec.a.length
        val localA = ec.a
        localA += null

        val bodyFn = body.eval(ec.copy(st = ec.st + (param -> (localIdx, lhs.`type`.asInstanceOf[TContainer].elementType))))
        val g = (x: Any) => {
          localA(localIdx) = x
          bodyFn()
        }

        AST.evalCompose[t](ec, lhs) { x1 => f(x1, g) }
      case f: Arity3Fun[_, _, _, _] =>
        AST.evalCompose(ec, lhs, args(0), args(1))(f)
      case f: Arity4Fun[_, _, _, _, _] =>
        AST.evalCompose(ec, lhs, args(0), args(1), args(2))(f)
      case fn =>
        throw new RuntimeException(s"Internal hail error, bad binding in function registry for `$name' with argument types $typ, $typs: $fn")
    }
  }

  def lookupFun(ec: EvalContext)(name: String, typs: Seq[Type])(args: Seq[AST]): Err[() => Any] = {
    require(typs.length == args.length)

    lookup(name, FunType(typs: _*)).map {
      case f: UnaryFun[_, _] =>
        AST.evalCompose(ec, args(0))(f)
      case f: OptionUnaryFun[_, _] =>
        AST.evalFlatCompose(ec, args(0))(f)
      case f: BinaryFun[_, _, _] =>
        AST.evalCompose(ec, args(0), args(1))(f)
      case f: Arity3Fun[_, _, _, _] =>
        AST.evalCompose(ec, args(0), args(1), args(2))(f)
      case f: Arity4Fun[_, _, _, _, _] =>
        AST.evalCompose(ec, args(0), args(1), args(2), args(3))(f)
      case fn =>
        throw new RuntimeException(s"Internal hail error, bad binding in function registry for `$name' with argument types $typs: $fn")
    }
  }

  def lookupFunReturnType(name: String, typs: Seq[Type]): Err[Type] =
    lookup(name, FunType(typs: _*)).map(_.retType)

  def registerMethod[T, U](name: String, impl: T => U)
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, MethodType(hrt.typ), UnaryFun[T, U](hru.typ, impl))
  }

  def registerMethod[T, U, V](name: String, impl: (T, U) => V)
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    bind(name, MethodType(hrt.typ, hru.typ), BinaryFun[T, U, V](hrv.typ, impl))
  }

  def registerLambdaMethod[T, U, V](name: String, impl: (T, (Any) => Any) => V)
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    val m = BinaryLambdaFun[T, U, V](hrv.typ, impl)
    println("registerLambdaMethod", m)
    bind(name, MethodType(hrt.typ, hru.typ), m)
  }

  def registerMethod[T, U, V, W](name: String, impl: (T, U, V) => W)
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W]) = {
    bind(name, MethodType(hrt.typ, hru.typ, hrv.typ), Arity3Fun[T, U, V, W](hrw.typ, impl))
  }

  def register[T, U](name: String, impl: T => U)
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, FunType(hrt.typ), UnaryFun[T, U](hru.typ, impl))
  }

  def registerOptionMethod[T, U](name: String, impl: T => Option[U])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, MethodType(hrt.typ), OptionUnaryFun[T, U](hru.typ, impl))
  }

  def registerOption[T, U](name: String, impl: T => Option[U])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, FunType(hrt.typ), OptionUnaryFun[T, U](hru.typ, impl))
  }

  def registerUnaryNAFilteredCollectionMethod[T, U](name: String, impl: TraversableOnce[T] => U)
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, MethodType(TArray(hrt.typ)), UnaryFun[IndexedSeq[_], U](hru.typ, { (ts: IndexedSeq[_]) =>
      impl(ts.filter(t => t != null).map(_.asInstanceOf[T]))
    }))
    bind(name, MethodType(TSet(hrt.typ)), UnaryFun[Set[_], U](hru.typ, { (ts: Set[_]) =>
      impl(ts.filter(t => t != null).map(_.asInstanceOf[T]))
    }))
  }

  def register[T, U, V](name: String, impl: (T, U) => V)
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    bind(name, FunType(hrt.typ, hru.typ), BinaryFun[T, U, V](hrv.typ, impl))
  }

  def register[T, U, V, W](name: String, impl: (T, U, V) => W)
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W]) = {
    bind(name, FunType(hrt.typ, hru.typ, hrv.typ), Arity3Fun[T, U, V, W](hrw.typ, impl))
  }

  def register[T, U, V, W, X](name: String, impl: (T, U, V, W) => X)
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W], hrx: HailRep[X]) = {
    bind(name, FunType(hrt.typ, hru.typ, hrv.typ, hrw.typ), Arity4Fun[T, U, V, W, X](hrx.typ, impl))
  }

  def registerAnn[T](name: String, t: TStruct, impl: T => Annotation)
    (implicit hrt: HailRep[T]) = {
    register(name, impl)(hrt, new HailRep[Annotation] {
      def typ = t
    })
  }

  def registerAnn[T, U](name: String, t: TStruct, impl: (T, U) => Annotation)
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    register(name, impl)(hrt, hru, new HailRep[Annotation] {
      def typ = t
    })
  }

  def registerAnn[T, U, V](name: String, t: TStruct, impl: (T, U, V) => Annotation)
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    register(name, impl)(hrt, hru, hrv, new HailRep[Annotation] {
      def typ = t
    })
  }

  def registerAnn[T, U, V, W](name: String, t: TStruct, impl: (T, U, V, W) => Annotation)
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W]) = {
    register(name, impl)(hrt, hru, hrv, hrw, new HailRep[Annotation] {
      def typ = t
    })
  }

  val TT = TVariable()
  val TU = TVariable()
  val TV = TVariable()

  val TTHr = new HailRep[Any] {
    def typ = TT
  }
  val TUHr = new HailRep[Any] {
    def typ = TU
  }
  val TVHr = new HailRep[Any] {
    def typ = TV
  }

  registerOptionMethod("gt", { (x: Genotype) => x.gt })
  registerOptionMethod("gtj", { (x: Genotype) => x.gt.map(gtx => Genotype.gtPair(gtx).j) })
  registerOptionMethod("gtk", { (x: Genotype) => x.gt.map(gtx => Genotype.gtPair(gtx).k) })
  registerOptionMethod("ad", { (x: Genotype) => x.ad.map(a => a: IndexedSeq[Int]) })
  registerOptionMethod("dp", { (x: Genotype) => x.dp })
  registerOptionMethod("od", { (x: Genotype) => x.od })
  registerOptionMethod("gq", { (x: Genotype) => x.gq })
  registerOptionMethod("pl", { (x: Genotype) => x.pl.map(a => a: IndexedSeq[Int]) })
  registerOptionMethod("dosage", { (x: Genotype) => x.dosage.map(a => a: IndexedSeq[Double]) })
  registerMethod("isHomRef", { (x: Genotype) => x.isHomRef })
  registerMethod("isHet", { (x: Genotype) => x.isHet })
  registerMethod("isHomVar", { (x: Genotype) => x.isHomVar })
  registerMethod("isCalledNonRef", { (x: Genotype) => x.isCalledNonRef })
  registerMethod("isHetNonRef", { (x: Genotype) => x.isHetNonRef })
  registerMethod("isHetRef", { (x: Genotype) => x.isHetRef })
  registerMethod("isCalled", { (x: Genotype) => x.isCalled })
  registerMethod("isNotCalled", { (x: Genotype) => x.isNotCalled })
  registerOptionMethod("nNonRefAlleles", { (x: Genotype) => x.nNonRefAlleles })
  registerOptionMethod("pAB", { (x: Genotype) => x.pAB() })
  registerOptionMethod("fractionReadsRef", { (x: Genotype) => x.fractionReadsRef() })
  registerMethod("fakeRef", { (x: Genotype) => x.fakeRef })
  registerMethod("isDosage", { (x: Genotype) => x.isDosage })
  registerMethod("contig", { (x: Variant) => x.contig })
  registerMethod("start", { (x: Variant) => x.start })
  registerMethod("ref", { (x: Variant) => x.ref })
  registerMethod("altAlleles", { (x: Variant) => x.altAlleles })
  registerMethod("nAltAlleles", { (x: Variant) => x.nAltAlleles })
  registerMethod("nAlleles", { (x: Variant) => x.nAlleles })
  registerMethod("isBiallelic", { (x: Variant) => x.isBiallelic })
  registerMethod("nGenotypes", { (x: Variant) => x.nGenotypes })
  registerMethod("inXPar", { (x: Variant) => x.inXPar })
  registerMethod("inYPar", { (x: Variant) => x.inYPar })
  registerMethod("inXNonPar", { (x: Variant) => x.inXNonPar })
  registerMethod("inYNonPar", { (x: Variant) => x.inYNonPar })
  // assumes biallelic
  registerMethod("alt", { (x: Variant) => x.alt })
  registerMethod("altAllele", { (x: Variant) => x.altAllele })
  registerMethod("locus", { (x: Variant) => x.locus })
  registerMethod("contig", { (x: Locus) => x.contig })
  registerMethod("position", { (x: Locus) => x.position })
  registerMethod("start", { (x: Interval[Locus]) => x.start })
  registerMethod("end", { (x: Interval[Locus]) => x.end })
  registerMethod("ref", { (x: AltAllele) => x.ref })
  registerMethod("alt", { (x: AltAllele) => x.alt })
  registerMethod("isSNP", { (x: AltAllele) => x.isSNP })
  registerMethod("isMNP", { (x: AltAllele) => x.isMNP })
  registerMethod("isIndel", { (x: AltAllele) => x.isIndel })
  registerMethod("isInsertion", { (x: AltAllele) => x.isInsertion })
  registerMethod("isDeletion", { (x: AltAllele) => x.isDeletion })
  registerMethod("isComplex", { (x: AltAllele) => x.isComplex })
  registerMethod("isTransition", { (x: AltAllele) => x.isTransition })
  registerMethod("isTransversion", { (x: AltAllele) => x.isTransversion })
  registerMethod("isAutosomal", { (x: Variant) => x.isAutosomal })

  registerMethod("toInt", { (x: Int) => x })
  registerMethod("toLong", { (x: Int) => x.toLong })
  registerMethod("toFloat", { (x: Int) => x.toFloat })
  registerMethod("toDouble", { (x: Int) => x.toDouble })

  registerMethod("toInt", { (x: Long) => x.toInt })
  registerMethod("toLong", { (x: Long) => x })
  registerMethod("toFloat", { (x: Long) => x.toFloat })
  registerMethod("toDouble", { (x: Long) => x.toDouble })

  registerMethod("toInt", { (x: Float) => x.toInt })
  registerMethod("toLong", { (x: Float) => x.toLong })
  registerMethod("toFloat", { (x: Float) => x })
  registerMethod("toDouble", { (x: Float) => x.toDouble })

  registerMethod("toInt", { (x: Boolean) => if (x) 1 else 0 })

  registerMethod("toInt", { (x: Double) => x.toInt })
  registerMethod("toLong", { (x: Double) => x.toLong })
  registerMethod("toFloat", { (x: Double) => x.toFloat })
  registerMethod("toDouble", { (x: Double) => x })

  registerMethod("toInt", { (x: String) => x.toInt })
  registerMethod("toLong", { (x: String) => x.toLong })
  registerMethod("toFloat", { (x: String) => x.toFloat })
  registerMethod("toDouble", { (x: String) => x.toDouble })

  registerMethod("abs", { (x: Int) => x.abs })
  registerMethod("abs", { (x: Long) => x.abs })
  registerMethod("abs", { (x: Float) => x.abs })
  registerMethod("abs", { (x: Double) => x.abs })

  registerMethod("signum", { (x: Double) => x.signum })
  registerMethod("length", { (x: String) => x.length })

  registerUnaryNAFilteredCollectionMethod("sum", { (x: TraversableOnce[Int]) => x.sum })
  registerUnaryNAFilteredCollectionMethod("sum", { (x: TraversableOnce[Long]) => x.sum })
  registerUnaryNAFilteredCollectionMethod("sum", { (x: TraversableOnce[Float]) => x.sum })
  registerUnaryNAFilteredCollectionMethod("sum", { (x: TraversableOnce[Double]) => x.sum })

  registerUnaryNAFilteredCollectionMethod("min", { (x: TraversableOnce[Int]) => x.min })
  registerUnaryNAFilteredCollectionMethod("min", { (x: TraversableOnce[Long]) => x.min })
  registerUnaryNAFilteredCollectionMethod("min", { (x: TraversableOnce[Float]) => x.min })
  registerUnaryNAFilteredCollectionMethod("min", { (x: TraversableOnce[Double]) => x.min })

  registerUnaryNAFilteredCollectionMethod("max", { (x: TraversableOnce[Int]) => x.max })
  registerUnaryNAFilteredCollectionMethod("max", { (x: TraversableOnce[Long]) => x.max })
  registerUnaryNAFilteredCollectionMethod("max", { (x: TraversableOnce[Float]) => x.max })
  registerUnaryNAFilteredCollectionMethod("max", { (x: TraversableOnce[Double]) => x.max })

  register("range", { (x: Int) =>
    val l = math.max(x, 0)
    new IndexedSeq[Int] {
      def length = l

      def apply(i: Int): Int = {
        if (i < 0 || i >= l)
          throw new ArrayIndexOutOfBoundsException(i)
        i
      }
    }
  })
  register("range", { (x: Int, y: Int) =>
    val l = math.max(y - x, 0)
    new IndexedSeq[Int] {
      def length = l

      def apply(i: Int): Int = {
        if (i < 0 || i >= l)
          throw new ArrayIndexOutOfBoundsException(i)
        x + i
      }
    }
  })
  register("range", { (x: Int, y: Int, step: Int) => x until y by step: IndexedSeq[Int] })
  register("Variant", { (x: String) =>
    val Array(chr, pos, ref, alts) = x.split(":")
    Variant(chr, pos.toInt, ref, alts.split(","))
  })
  register("Variant", { (x: String, y: Int, z: String, a: String) => Variant(x, y, z, a) })
  register("Variant", { (x: String, y: Int, z: String, a: IndexedSeq[String]) => Variant(x, y, z, a.toArray) })

  register("Locus", { (x: String) =>
    val Array(chr, pos) = x.split(":")
    Locus(chr, pos.toInt)
  })
  register("Locus", { (x: String, y: Int) => Locus(x, y) })
  register("Interval", { (x: Locus, y: Locus) => Interval(x, y) })
  registerAnn("hwe", TStruct(("rExpectedHetFrequency", TDouble), ("pHWE", TDouble)), { (nHomRef: Int, nHet: Int, nHomVar: Int) =>
    if (nHomRef < 0 || nHet < 0 || nHomVar < 0)
      fatal(s"got invalid (negative) argument to function `hwe': hwe($nHomRef, $nHet, $nHomVar)")
    val n = nHomRef + nHet + nHomVar
    val nAB = nHet
    val nA = nAB + 2 * nHomRef.min(nHomVar)

    val LH = LeveneHaldane(n, nA)
    Annotation(divOption(LH.getNumericalMean, n).orNull, LH.exactMidP(nAB))
  })
  registerAnn("fet", TStruct(("pValue", TDouble), ("oddsRatio", TDouble), ("ci95Lower", TDouble), ("ci95Upper", TDouble)), { (c1: Int, c2: Int, c3: Int, c4: Int) =>
    if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0)
      fatal(s"got invalid argument to function `fet': fet($c1, $c2, $c3, $c4)")
    val fet = FisherExactTest(c1, c2, c3, c4)
    Annotation(fet(0).orNull, fet(1).orNull, fet(2).orNull, fet(3).orNull)
  })
  // NB: merge takes two structs, how do I deal with structs?
  register("exp", { (x: Double) => math.exp(x) })
  register("log10", { (x: Double) => math.log10(x) })
  register("sqrt", { (x: Double) => math.sqrt(x) })
  register("log", (x: Double) => math.log(x))
  register("log", (x: Double, b: Double) => math.log(x) / math.log(b))
  register("pow", (b: Double, x: Double) => math.pow(b, x))

  register("pcoin", { (p: Double) => math.random < p })
  register("runif", { (min: Double, max: Double) => min + (max - min) * math.random })
  register("rnorm", { (mean: Double, sd: Double) => mean + sd * scala.util.Random.nextGaussian() })

  registerConversion((x: Int) => x.toDouble, priority = 2)
  registerConversion { (x: Long) => x.toDouble }
  registerConversion { (x: Int) => x.toLong }
  registerConversion { (x: Float) => x.toDouble }

  register("gtj", (i: Int) => Genotype.gtPair(i).j)
  register("gtk", (i: Int) => Genotype.gtPair(i).k)
  register("gtIndex", (j: Int, k: Int) => Genotype.gtIndex(j, k))

  registerMethod("split", (s: String, p: String) => s.split(p): IndexedSeq[String])

  registerMethod("oneHotAlleles", (g: Genotype, v: Variant) => g.oneHotAlleles(v).orNull)

  registerMethod("oneHotGenotype", (g: Genotype, v: Variant) => g.oneHotGenotype(v).orNull)

  registerMethod("replace", (str: String, pattern1: String, pattern2: String) =>
    str.replaceAll(pattern1, pattern2))

  registerMethod("contains", (interval: Interval[Locus], locus: Locus) => interval.contains(locus))

  registerMethod("min", (a: Int, b: Int) => a.min(b))
  registerMethod("min", (a: Long, b: Long) => a.min(b))
  registerMethod("min", (a: Float, b: Float) => a.min(b))
  registerMethod("min", (a: Double, b: Double) => a.min(b))

  registerMethod("max", (a: Int, b: Int) => a.max(b))
  registerMethod("max", (a: Long, b: Long) => a.max(b))
  registerMethod("max", (a: Float, b: Float) => a.max(b))
  registerMethod("max", (a: Double, b: Double) => a.max(b))

  registerMethod("length", (a: IndexedSeq[Any]) => a.length)(arrayHr(TTHr), intHr)
  registerMethod("size", (a: IndexedSeq[Any]) => a.size)(arrayHr(TTHr), intHr)
  registerMethod("size", (s: Set[Any]) => s.size)(setHr(TTHr), intHr)
  registerMethod("size", (d: Map[String, Any]) => d.size)(dictHr(TTHr), intHr)

  registerMethod("id", (s: String) => s)(sampleHr, stringHr)

  registerMethod("isEmpty", (a: IndexedSeq[Any]) => a.isEmpty)(arrayHr(TTHr), boolHr)
  registerMethod("isEmpty", (s: Set[Any]) => s.isEmpty)(setHr(TTHr), boolHr)
  registerMethod("isEmpty", (d: Map[String, Any]) => d.isEmpty)(dictHr(TTHr), boolHr)

  registerMethod("toSet", (a: IndexedSeq[Any]) => a.toSet)(arrayHr(TTHr), setHr(TTHr))
  registerMethod("toArray", (a: Set[Any]) => a.toArray[Any]: IndexedSeq[Any])(setHr(TTHr), arrayHr(TTHr))

  registerMethod("head", (a: IndexedSeq[Any]) => a.head)(arrayHr(TTHr), TTHr)
  registerMethod("tail", (a: IndexedSeq[Any]) => a.tail)(arrayHr(TTHr), arrayHr(TTHr))

  registerMethod("flatten", (a: IndexedSeq[IndexedSeq[Any]]) =>
    flattenOrNull[IndexedSeq, Any](IndexedSeq.newBuilder[Any], a)
  )(arrayHr(arrayHr(TTHr)), arrayHr(TTHr))

  registerMethod("flatten", (s: Set[Set[Any]]) =>
    flattenOrNull[Set, Any](Set.newBuilder[Any], s)
  )(setHr(setHr(TTHr)), setHr(TTHr))

  registerMethod("mkString", (a: IndexedSeq[String], d: String) => a.mkString(d))(
    arrayHr(stringHr), stringHr, stringHr)
  registerMethod("mkString", (s: Set[String], d: String) => s.mkString(d))(
    setHr(stringHr), stringHr, stringHr)

  registerMethod("contains", (s: Set[Any], x: Any) => s.contains(x))(setHr(TTHr), TTHr, boolHr)
  registerMethod("contains", (d: Map[String, Any], x: String) => d.contains(x))(dictHr(TTHr), stringHr, boolHr)

  registerLambdaMethod("find", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.find { elt =>
      val r = f(elt)
      r != null && r.asInstanceOf[Boolean]
    }.orNull
  )(arrayHr(TTHr), unaryHr(TTHr, boolHr), TTHr)

  registerLambdaMethod("find", (s: Set[Any], f: (Any) => Any) =>
    s.find { elt =>
      val r = f(elt)
      r != null && r.asInstanceOf[Boolean]
    }.orNull
  )(setHr(TTHr), unaryHr(TTHr, boolHr), TTHr)

  registerLambdaMethod("map", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.map(f)
  )(arrayHr(TTHr), unaryHr(TTHr, TUHr), arrayHr(TUHr))

  registerLambdaMethod("map", (s: Set[Any], f: (Any) => Any) =>
    s.map(f)
  )(setHr(TTHr), unaryHr(TTHr, TUHr), setHr(TUHr))

  registerLambdaMethod("mapValues", (a: Map[String, Any], f: (Any) => Any) =>
    a.mapValues(f)
  )(dictHr(TTHr), unaryHr(TTHr, TUHr), dictHr(TUHr))

  registerLambdaMethod("flatMap", (a: IndexedSeq[Any], f: (Any) => Any) =>
    flattenOrNull[IndexedSeq, Any](IndexedSeq.newBuilder[Any],
      a.map(f).asInstanceOf[IndexedSeq[IndexedSeq[Any]]])
  )(arrayHr(TTHr), unaryHr(TTHr, arrayHr(TUHr)), arrayHr(TUHr))

  registerLambdaMethod("flatMap", (s: Set[Any], f: (Any) => Any) =>
    flattenOrNull[Set, Any](Set.newBuilder[Any],
      s.map(f).asInstanceOf[Set[Set[Any]]])
  )(setHr(TTHr), unaryHr(TTHr, setHr(TUHr)), setHr(TUHr))

  registerLambdaMethod("exists", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.exists { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }
  )(arrayHr(TTHr), unaryHr(TTHr, boolHr), boolHr)

  registerLambdaMethod("exists", (s: Set[Any], f: (Any) => Any) =>
    s.exists { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }
  )(setHr(TTHr), unaryHr(TTHr, boolHr), boolHr)

  registerLambdaMethod("forall", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.forall { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }
  )(arrayHr(TTHr), unaryHr(TTHr, boolHr), boolHr)

  registerLambdaMethod("forall", (s: Set[Any], f: (Any) => Any) =>
    s.forall { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }
  )(setHr(TTHr), unaryHr(TTHr, boolHr), boolHr)

  registerLambdaMethod("filter", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.filter { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }
  )(arrayHr(TTHr), unaryHr(TTHr, boolHr), arrayHr(TTHr))

  registerLambdaMethod("filter", (s: Set[Any], f: (Any) => Any) =>
    s.filter { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }
  )(setHr(TTHr), unaryHr(TTHr, boolHr), setHr(TTHr))
}
