package is.hail.expr.ir

import is.hail.ExecStrategy
import is.hail.SparkSuite
import is.hail.annotations._
import is.hail.check.{Gen, Prop}
import is.hail.asm4s._
import is.hail.TestUtils._
import is.hail.expr.types.physical._
import is.hail.expr.types.virtual._
import is.hail.utils._
import org.apache.spark.sql.Row
import org.testng.annotations.{DataProvider, Test}

class OrderingSuite extends SparkSuite {

  implicit val execStrats = ExecStrategy.values

  def recursiveSize(t: Type): Int = {
    val inner = t match {
      case ti: TInterval => recursiveSize(ti.pointType)
      case tc: TIterable => recursiveSize(tc.elementType)
      case tbs: TBaseStruct =>
        tbs.types.map { t => recursiveSize(t) }.sum
      case _ => 0
    }
    inner + 1
  }

  def getStagedOrderingFunction[T: TypeInfo](t: Type, comp: String): AsmFunction3[Region, Long, Long, T] = {
    val fb = EmitFunctionBuilder[Region, Long, Long, T]
    val stagedOrdering = t.physicalType.codeOrdering(fb.apply_method)
    val cregion: Code[Region] = fb.getArg[Region](1)
    val cv1 = coerce[stagedOrdering.T](cregion.getIRIntermediate(t)(fb.getArg[Long](2)))
    val cv2 = coerce[stagedOrdering.T](cregion.getIRIntermediate(t)(fb.getArg[Long](3)))
    comp match {
      case "compare" => fb.emit(stagedOrdering.compare(cregion, (const(false), cv1), cregion, (const(false), cv2)))
      case "equiv" => fb.emit(stagedOrdering.equiv(cregion, (const(false), cv1), cregion, (const(false), cv2)))
      case "lt" => fb.emit(stagedOrdering.lt(cregion, (const(false), cv1), cregion, (const(false), cv2)))
      case "lteq" => fb.emit(stagedOrdering.lteq(cregion, (const(false), cv1), cregion, (const(false), cv2)))
      case "gt" => fb.emit(stagedOrdering.gt(cregion, (const(false), cv1), cregion, (const(false), cv2)))
      case "gteq" => fb.emit(stagedOrdering.gteq(cregion, (const(false), cv1), cregion, (const(false), cv2)))
    }
    fb.resultWithIndex()(0)
  }

  def addTupledArgsToRegion(region: Region, args: (Type, Annotation)*): Array[Long] = {
    val rvb = new RegionValueBuilder(region)
    args.map { case (t, a) =>
      rvb.start(TTuple(t).physicalType)
      rvb.startTuple()
      rvb.addAnnotation(t, a)
      rvb.endTuple()
      rvb.end()
    }.toArray
  }

  @Test def testRandomOpsAgainstExtended() {
    val compareGen = for {
      t <- Type.genArb
      a1 <- t.genNonmissingValue
      a2 <- t.genNonmissingValue
    } yield (t, a1, a2)
    val p = Prop.forAll(compareGen) { case (t, a1, a2) =>
      Region.scoped { region =>
        val rvb = new RegionValueBuilder(region)

        rvb.start(t.physicalType)
        rvb.addAnnotation(t, a1)
        val v1 = rvb.end()

        rvb.start(t.physicalType)
        rvb.addAnnotation(t, a2)
        val v2 = rvb.end()

        val compare = java.lang.Integer.signum(t.ordering.compare(a1, a2))
        val fcompare = getStagedOrderingFunction[Int](t, "compare")
        val result = java.lang.Integer.signum(fcompare(region, v1, v2))

        assert(result == compare, s"compare expected: $compare vs $result")


        val equiv = t.ordering.equiv(a1, a2)
        val fequiv = getStagedOrderingFunction[Boolean](t, "equiv")

        assert(fequiv(region, v1, v2) == equiv, s"equiv expected: $equiv")

        val lt = t.ordering.lt(a1, a2)
        val flt = getStagedOrderingFunction[Boolean](t, "lt")

        assert(flt(region, v1, v2) == lt, s"lt expected: $lt")

        val lteq = t.ordering.lteq(a1, a2)
        val flteq = getStagedOrderingFunction[Boolean](t, "lteq")

        assert(flteq(region, v1, v2) == lteq, s"lteq expected: $lteq")

        val gt = t.ordering.gt(a1, a2)
        val fgt = getStagedOrderingFunction[Boolean](t, "gt")

        assert(fgt(region, v1, v2) == gt, s"gt expected: $gt")

        val gteq = t.ordering.gteq(a1, a2)
        val fgteq = getStagedOrderingFunction[Boolean](t, "gteq")

        assert(fgteq(region, v1, v2) == gteq, s"gteq expected: $gteq")
      }

      true
    }
    p.check()
  }

  @Test def testSortOnRandomArray() {
    implicit val execStrats = ExecStrategy.javaOnly
    val compareGen = for {
      elt <- Type.genArb
      a <- TArray(elt).genNonmissingValue
      asc <- Gen.coin()
    } yield (elt, a, asc)
    val p = Prop.forAll(compareGen) { case (t, a: IndexedSeq[Any], asc: Boolean) =>
      val ord = if (asc) t.ordering.toOrdering else t.ordering.reverse.toOrdering
      assertEvalsTo(ArraySort(In(0, TArray(t)), Literal.coerce(TBoolean(), asc)),
        FastIndexedSeq(a -> TArray(t)),
        expected = a.sorted(ord))
      true
    }
    p.check()
  }

  @Test def testToSetOnRandomDuplicatedArray() {
    implicit val execStrats = ExecStrategy.javaOnly
    val compareGen = for {
      elt <- Type.genArb
      a <- TArray(elt).genNonmissingValue
    } yield (elt, a)
    val p = Prop.forAll(compareGen) { case (t, a: IndexedSeq[Any]) =>
      val array = a ++ a
      assertEvalsTo(ToArray(ToSet(In(0, TArray(t)))),
        FastIndexedSeq(array -> TArray(t)),
        expected = array.sorted(t.ordering.toOrdering).distinct)
      true
    }
    p.check()
  }

  @Test def testToDictOnRandomDuplicatedArray() {
    implicit val execStrats = ExecStrategy.javaOnly
    val compareGen = for {
      kt <- Type.genArb
      vt <- Type.genArb
      telt = TTuple(kt, vt)
      a <- TArray(telt).genNonmissingValue
    } yield (telt, a)
    val p = Prop.forAll(compareGen) { case (telt: TTuple, a: IndexedSeq[Row]@unchecked) =>
      val tdict = TDict(telt.types(0), telt.types(1))
      val array: IndexedSeq[Row] = a ++ a
      val expectedMap = array.filter(_ != null).map { case Row(k, v) => (k, v) }.toMap
      assertEvalsTo(
        ArrayMap(ToArray(ToDict(In(0, TArray(telt)))),
        "x", GetField(Ref("x", -tdict.elementType), "key")),
        FastIndexedSeq(array -> TArray(telt)),
        expected = expectedMap.keys.toFastIndexedSeq.sorted(telt.types(0).ordering.toOrdering))
      true
    }
    p.check()
  }

  @Test def testSortOnMissingArray() {
    implicit val execStrats = ExecStrategy.javaOnly
    val tarray = TArray(TStruct("key" -> TInt32(), "value" -> TInt32()))
    val irs: Array[IR => IR] = Array(ArraySort(_, True()), ToSet(_), ToDict(_))

    for (irF <- irs) { assertEvalsTo(IsNA(irF(NA(tarray))), true) }
  }

  @Test def testSetContainsOnRandomSet() {
    implicit val execStrats = ExecStrategy.javaOnly
    val compareGen = Type.genArb
      .flatMap(t => Gen.zip(Gen.const(TSet(t)), TSet(t).genNonmissingValue, t.genValue))
    val p = Prop.forAll(compareGen) { case (tset: TSet, set: Set[Any]@unchecked, test1) =>
      val telt = tset.elementType

      if (set.nonEmpty) {
        assertEvalsTo(
          invoke("contains", In(0, tset), In(1, telt)),
          FastIndexedSeq(set -> tset, set.head -> telt),
          expected = true)
      }

      assertEvalsTo(
        invoke("contains", In(0, tset), In(1, telt)),
        FastIndexedSeq(set -> tset, test1 -> telt),
        expected = set.contains(test1))
      true
    }
    p.check()
  }

  @Test def testDictGetOnRandomDict() {
    implicit val execStrats = ExecStrategy.javaOnly

    val compareGen = Gen.zip(Type.genArb, Type.genArb).flatMap {
      case (k, v) =>
        Gen.zip(Gen.const(TDict(k, v)), TDict(k, v).genNonmissingValue, k.genNonmissingValue)
    }
    val p = Prop.forAll(compareGen) { case (tdict: TDict, dict: Map[Any, Any]@unchecked, testKey1) =>
      assertEvalsTo(invoke("get", In(0, tdict), In(1, -tdict.keyType)),
        FastIndexedSeq(dict -> tdict,
          testKey1 -> -tdict.keyType),
        dict.getOrElse(testKey1, null))

      if (dict.nonEmpty) {
        val testKey2 = dict.keys.toSeq.head
        val expected2 = dict(testKey2)
        assertEvalsTo(invoke("get", In(0, tdict), In(1, -tdict.keyType)),
          FastIndexedSeq(dict -> tdict,
            testKey2 -> -tdict.keyType),
          expected2)
      }
      true
    }
    p.check()
  }

  @Test def testBinarySearchOnSet() {
    val compareGen = Type.genArb.flatMap(t => Gen.zip(Gen.const(t), TSet(t).genNonmissingValue, t.genNonmissingValue))
    val p = Prop.forAll(compareGen.filter { case (t, a, elem) => a.asInstanceOf[Set[Any]].nonEmpty }) { case (t, a, elem) =>
      val set = a.asInstanceOf[Set[Any]]
      val pset = PSet(t.physicalType)

      Region.scoped { region =>
        val rvb = new RegionValueBuilder(region)

        rvb.start(pset)
        rvb.addAnnotation(pset.virtualType, set)
        val soff = rvb.end()

        rvb.start(TTuple(t).physicalType)
        rvb.addAnnotation(TTuple(t), Row(elem))
        val eoff = rvb.end()

        val fb = EmitFunctionBuilder[Region, Long, Long, Int]
        val cregion = fb.getArg[Region](1).load()
        val cset = fb.getArg[Long](2)
        val cetuple = fb.getArg[Long](3)

        val bs = new BinarySearch(fb.apply_method, pset, keyOnly = false)
        fb.emit(bs.getClosestIndex(cset, false, cregion.loadIRIntermediate(t)(TTuple(t).physicalType.fieldOffset(cetuple, 0))))

        val asArray = SafeIndexedSeq(TArray(t).physicalType, region, soff)

        val f = fb.resultWithIndex()(0)
        val closestI = f(region, soff, eoff)
        val maybeEqual = asArray(closestI)

        set.contains(elem) ==> (elem == maybeEqual) &&
          (t.ordering.compare(elem, maybeEqual) <= 0 || (closestI == set.size - 1))
      }
    }
    p.check()
  }

  @Test def testBinarySearchOnDict() {
    val compareGen = Gen.zip(Type.genArb, Type.genArb)
      .flatMap { case (k, v) => Gen.zip(Gen.const(TDict(k, v)), TDict(k, v).genNonmissingValue, k.genValue) }
    val p = Prop.forAll(compareGen.filter { case (tdict, a, key) => a.asInstanceOf[Map[Any, Any]].nonEmpty }) { case (tDict, a, key) =>
      val dict = a.asInstanceOf[Map[Any, Any]]
      val pDict = tDict.physicalType

      Region.scoped { region =>
        val rvb = new RegionValueBuilder(region)

        rvb.start(pDict)
        rvb.addAnnotation(tDict, dict)
        val soff = rvb.end()

        val ptuple = PTuple(FastIndexedSeq(pDict.keyType))
        rvb.start(ptuple)
        rvb.addAnnotation(ptuple.virtualType, Row(key))
        val eoff = rvb.end()

        val fb = EmitFunctionBuilder[Region, Long, Long, Int]
        val cregion = fb.getArg[Region](1).load()
        val cdict = fb.getArg[Long](2)
        val cktuple = fb.getArg[Long](3)

        val bs = new BinarySearch(fb.apply_method, pDict, keyOnly = true)
        val m = ptuple.isFieldMissing(cregion, cktuple, 0)
        val v = cregion.loadIRIntermediate(pDict.keyType)(ptuple.fieldOffset(cktuple, 0))
        fb.emit(bs.getClosestIndex(cdict, m, v))

        val asArray = SafeIndexedSeq(PArray(pDict.elementType), region, soff)

        val f = fb.resultWithIndex()(0)
        val closestI = f(region, soff, eoff)

        if (closestI == asArray.length) {
          !dict.contains(key) ==> asArray.forall { keyI =>
            val otherKey = keyI.asInstanceOf[Row].get(0)
            pDict.keyType.virtualType.ordering.compare(key, otherKey) > 0
          }
        } else {
          def getKey(i: Int) = asArray(i).asInstanceOf[Row].get(0)
          val maybeEqual = getKey(closestI)
          val closestIIsClosest =
            (pDict.keyType.virtualType.ordering.compare(key, maybeEqual) <= 0 || closestI == dict.size - 1) &&
              (closestI == 0 || pDict.keyType.virtualType.ordering.compare(key, getKey(closestI - 1)) > 0)

          dict.contains(key) ==> (key == maybeEqual) && closestIIsClosest
        }
      }
    }
    p.check()
  }

  @Test def testContainsWithArrayFold() {
    implicit val execStrats = ExecStrategy.javaOnly
    val set1 = ToSet(MakeArray(Seq(I32(1), I32(4)), TArray(TInt32())))
    val set2 = ToSet(MakeArray(Seq(I32(9), I32(1), I32(4)), TArray(TInt32())))
    assertEvalsTo(ArrayFold(ToArray(set1), True(), "accumulator", "setelt",
        ApplySpecial("&&",
          FastSeq(
            Ref("accumulator", TBoolean()),
            invoke("contains", set2, Ref("setelt", TInt32()))))), true)
  }

  @DataProvider(name = "arrayDoubleOrderingData")
  def arrayDoubleOrderingData(): Array[Array[Any]] = {
    val xs = Array[Any](null, Double.NegativeInfinity, -0.0, 0.0, 1.0, Double.PositiveInfinity, Double.NaN)

    val as = Array(null: IndexedSeq[Any]) ++
      (for (x <- xs) yield IndexedSeq[Any](x))

    for (a <- as; a2 <- as)
      yield Array[Any](a, a2)
  }

  @Test(dataProvider = "arrayDoubleOrderingData")
  def testOrderingArrayDouble(
    a: IndexedSeq[Any], a2: IndexedSeq[Any]) {
    val t = TArray(TFloat64())

    val args = FastIndexedSeq(a -> t, a2 -> t)

    assertEvalSame(ApplyComparisonOp(EQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(EQWithNA(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(NEQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(NEQWithNA(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(LT(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(LTEQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(GT(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(GTEQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(Compare(t, t), In(0, t), In(1, t)), args)
  }

  @Test(dataProvider = "arrayDoubleOrderingData")
  def testOrderingSetDouble(
    a: IndexedSeq[Any], a2: IndexedSeq[Any]) {
    val t = TSet(TFloat64())

    val s = if (a != null) a.toSet else null
    val s2 = if (a2 != null) a2.toSet else null
    val args = FastIndexedSeq(s -> t, s2 -> t)

    assertEvalSame(ApplyComparisonOp(EQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(EQWithNA(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(NEQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(NEQWithNA(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(LT(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(LTEQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(GT(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(GTEQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(Compare(t, t), In(0, t), In(1, t)), args)
  }

  @DataProvider(name = "rowDoubleOrderingData")
  def rowDoubleOrderingData(): Array[Array[Any]] = {
    val xs = Array[Any](null, Double.NegativeInfinity, -0.0, 0.0, 1.0, Double.PositiveInfinity, Double.NaN)
    val as = Array(null: IndexedSeq[Any]) ++
      (for (x <- xs) yield FastIndexedSeq[Any](x))
    val ss = Array[Any](null, "a", "aa")

    val rs = for (x <- xs; s <- ss)
      yield Row(x, s)

    for (r <- rs; r2 <- rs)
      yield Array[Any](r, r2)
  }

  @Test(dataProvider = "rowDoubleOrderingData")
  def testOrderingRowDouble(
    r: Row, r2: Row) {
    val t = TStruct("x" -> TFloat64(), "s" -> TString())

    val args = FastIndexedSeq(r -> t, r2 -> t)

    assertEvalSame(ApplyComparisonOp(EQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(EQWithNA(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(NEQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(NEQWithNA(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(LT(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(LTEQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(GT(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(GTEQ(t, t), In(0, t), In(1, t)), args)
    assertEvalSame(ApplyComparisonOp(Compare(t, t), In(0, t), In(1, t)), args)
  }
}
