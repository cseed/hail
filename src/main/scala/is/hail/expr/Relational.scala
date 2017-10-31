package is.hail.expr

import is.hail.HailContext
import is.hail.annotations._
import is.hail.methods.Aggregators
import is.hail.sparkextras._
import is.hail.variant.{GenomeReference, VSMLocalValue}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import is.hail.utils._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.json4s.jackson.JsonMethods

import scala.reflect.ClassTag

case class MatrixType(
  globalType: Type = TStruct.empty,
  sType: Type = TString,
  saType: Type = TStruct.empty,
  vType: Type = TVariant(GenomeReference.GRCh37),
  vaType: Type = TStruct.empty,
  gType: Type = TGenotype) extends BaseType {

  def locusType: Type = vType match {
    case t: TVariant => TLocus(t.gr)
    case _ => vType
  }

  def rowType: TStruct =
    TStruct(
      "pk" -> locusType,
      "v" -> vType,
      "va" -> vaType,
      "gs" -> TArray(gType))

  def orderedRDD2Type: OrderedRDD2Type = {
    new OrderedRDD2Type(Array("pk"),
      Array("pk", "v"),
      rowType)
  }

  def pkType: TStruct = orderedRDD2Type.pkType

  def kType: TStruct = orderedRDD2Type.kType

  def sampleEC: EvalContext = {
    val aggregationST = Map(
      "global" -> (0, globalType),
      "s" -> (1, sType),
      "sa" -> (2, saType),
      "g" -> (3, gType),
      "v" -> (4, vType),
      "va" -> (5, vaType))
    EvalContext(Map(
      "global" -> (0, globalType),
      "s" -> (1, sType),
      "sa" -> (2, saType),
      "gs" -> (3, TAggregable(gType, aggregationST))))
  }

  def variantEC: EvalContext = {
    val aggregationST = Map(
      "global" -> (0, globalType),
      "v" -> (1, vType),
      "va" -> (2, vaType),
      "g" -> (3, gType),
      "s" -> (4, sType),
      "sa" -> (5, saType))
    EvalContext(Map(
      "global" -> (0, globalType),
      "v" -> (1, vType),
      "va" -> (2, vaType),
      "gs" -> (3, TAggregable(gType, aggregationST))))
  }

  def genotypeEC: EvalContext = {
    EvalContext(Map(
      "global" -> (0, globalType),
      "v" -> (1, vType),
      "va" -> (2, vaType),
      "s" -> (3, sType),
      "sa" -> (4, saType),
      "g" -> (5, gType)))
  }
}

object BaseIR {
  def genericRewriteTopDown(ast: BaseIR, rule: PartialFunction[BaseIR, BaseIR]): BaseIR = {
    def rewrite(ast: BaseIR): BaseIR = {
      rule.lift(ast) match {
        case Some(newAST) if newAST != ast =>
          rewrite(newAST)
        case None =>
          val newChildren = ast.children.map(rewrite)
          if ((ast.children, newChildren).zipped.forall(_ eq _))
            ast
          else
            ast.copy(newChildren)
      }
    }

    rewrite(ast)
  }

  def rewriteTopDown(ast: MatrixIR, rule: PartialFunction[BaseIR, BaseIR]): MatrixIR =
    genericRewriteTopDown(ast, rule).asInstanceOf[MatrixIR]

  def rewriteTopDown(ast: KeyTableIR, rule: PartialFunction[BaseIR, BaseIR]): KeyTableIR =
    genericRewriteTopDown(ast, rule).asInstanceOf[KeyTableIR]

  def genericRewriteBottomUp(ast: BaseIR, rule: PartialFunction[BaseIR, BaseIR]): BaseIR = {
    def rewrite(ast: BaseIR): BaseIR = {
      val newChildren = ast.children.map(rewrite)

      // only recons if necessary
      val rewritten =
        if ((ast.children, newChildren).zipped.forall(_ eq _))
          ast
        else
          ast.copy(newChildren)

      rule.lift(rewritten) match {
        case Some(newAST) if newAST != rewritten =>
          rewrite(newAST)
        case None =>
          rewritten
      }
    }

    rewrite(ast)
  }

  def rewriteBottomUp(ast: MatrixIR, rule: PartialFunction[BaseIR, BaseIR]): MatrixIR =
    genericRewriteBottomUp(ast, rule).asInstanceOf[MatrixIR]

  def rewriteBottomUp(ast: KeyTableIR, rule: PartialFunction[BaseIR, BaseIR]): KeyTableIR =
    genericRewriteBottomUp(ast, rule).asInstanceOf[KeyTableIR]
}

abstract class BaseIR {
  def typ: BaseType

  def children: IndexedSeq[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): BaseIR

  def mapChildren(f: (BaseIR) => BaseIR): BaseIR = {
    copy(children.map(f))
  }
}

object MatrixValue {
  def apply[RPK, RK, T](
    typ: MatrixType,
    localValue: VSMLocalValue,
    rdd: OrderedRDD[RPK, RK, (Any, Iterable[T])]): MatrixValue = {
    implicit val kOk: OrderedKey[Annotation, Annotation] = typ.vType.orderedKey
    val sc = rdd.sparkContext
    val localRowType = typ.rowType
    val localGType = typ.gType
    val localNSamples = localValue.nSamples
    val rangeBoundsType = TArray(typ.pkType)
    new MatrixValue(typ, localValue,
      OrderedRDD2(typ.orderedRDD2Type,
        new OrderedPartitioner2(rdd.orderedPartitioner.numPartitions,
          typ.orderedRDD2Type.partitionKey,
          typ.orderedRDD2Type.kType,
          UnsafeIndexedSeq(rangeBoundsType,
            rdd.orderedPartitioner.rangeBounds.map(b => Row(b)))),
        rdd.mapPartitions { it =>
          val region = MemoryBuffer()
          val rvb = new RegionValueBuilder(region)
          val rv = RegionValue(region)

          it.map { case (v, (va, gs)) =>
            region.clear()
            rvb.start(localRowType)
            rvb.startStruct()
            rvb.addAnnotation(localRowType.fieldType(0), kOk.project(v))
            rvb.addAnnotation(localRowType.fieldType(1), v)
            rvb.addAnnotation(localRowType.fieldType(2), va)
            rvb.startArray(localNSamples)
            var i = 0
            val git = gs.iterator
            while (git.hasNext) {
              rvb.addAnnotation(localGType, git.next())
              i += 1
            }
            rvb.endArray()
            rvb.endStruct()

            rv.setOffset(rvb.end())
            rv
          }
        }))
  }
}

case class MatrixValue(
  typ: MatrixType,
  localValue: VSMLocalValue,
  rdd2: OrderedRDD2) {
  def rdd[RPK, RK, T]: OrderedRDD[RPK, RK, (Any, Iterable[T])] = {
    warn("converting OrderedRDD2 => OrderedRDD")

    implicit val tct: ClassTag[T] = typ.gType.scalaClassTag.asInstanceOf[ClassTag[T]]
    implicit val kOk: OrderedKey[RPK, RK] = typ.vType.typedOrderedKey[RPK, RK]

    import kOk._

    val localRowType = typ.rowType
    val localNSamples = localValue.nSamples
    OrderedRDD(
      rdd2.map { rv =>
        val ur = new UnsafeRow(localRowType, rv.region.copy(), rv.offset)

        val gs = ur.getAs[IndexedSeq[T]](3)
        assert(gs.length == localNSamples)

        (ur.getAs[RK](1),
          (ur.get(2),
            ur.getAs[IndexedSeq[T]](3): Iterable[T]))
      },
      OrderedPartitioner(
        rdd2.orderedPartitioner.rangeBounds.map { b =>
          b.asInstanceOf[Row].getAs[RPK](0)
        }.toArray(kOk.pkct),
        rdd2.orderedPartitioner.numPartitions))
  }

  def copyRDD[RPK2, RK2, T2](typ: MatrixType = typ,
    localValue: VSMLocalValue = localValue,
    rdd: OrderedRDD[RPK2, RK2, (Any, Iterable[T2])]): MatrixValue = {
    MatrixValue(typ, localValue, rdd)
  }

  def sparkContext: SparkContext = rdd2.sparkContext

  def nPartitions: Int = rdd2.partitions.length

  def nSamples: Int = localValue.nSamples

  def globalAnnotation: Annotation = localValue.globalAnnotation

  def sampleIds: IndexedSeq[Annotation] = localValue.sampleIds

  def sampleAnnotations: IndexedSeq[Annotation] = localValue.sampleAnnotations

  lazy val sampleIdsBc: Broadcast[IndexedSeq[Annotation]] = sparkContext.broadcast(sampleIds)

  lazy val sampleAnnotationsBc: Broadcast[IndexedSeq[Annotation]] = sparkContext.broadcast(sampleAnnotations)

  def sampleIdsAndAnnotations: IndexedSeq[(Annotation, Annotation)] = sampleIds.zip(sampleAnnotations)

  def filterSamples(p: (Annotation, Annotation) => Boolean): MatrixValue = {
    val keep = sampleIdsAndAnnotations.zipWithIndex
      .filter { case ((s, sa), i) => p(s, sa) }
      .map(_._2)
    val keepBc = sparkContext.broadcast(keep)
    val localRowType = typ.rowType
    val localNSamples = nSamples
    copy(localValue =
      localValue.copy(
        sampleIds = keep.map(sampleIds),
        sampleAnnotations = keep.map(sampleAnnotations)),
      rdd2 = rdd2.mapPartitionsPreservesPartitioning { it =>
        var rv2b = new RegionValueBuilder()
        var rv2 = RegionValue()

        it.map { rv =>
          rv2b.set(rv.region)
          rv2b.start(localRowType)
          rv2b.startStruct()
          rv2b.addField(localRowType, rv, 0)
          rv2b.addField(localRowType, rv, 1)
          rv2b.addField(localRowType, rv, 2)

          rv2b.startArray(keepBc.value.length)
          val gst = localRowType.fieldType(3).asInstanceOf[TArray]
          val gt = gst.elementType
          val aoff = localRowType.loadField(rv, 3)
          var i = 0
          val length = keepBc.value.length
          while (i < length) {
            val j = keepBc.value(i)
            rv2b.addElement(gst, rv.region, aoff, j)
            i += 1
          }
          rv2b.endArray()
          rv2b.endStruct()
          rv2.set(rv.region, rv2b.end())

          rv2
        }
      })
  }
}

object MatrixIR {
  def optimize(ast: MatrixIR): MatrixIR = {
    BaseIR.rewriteTopDown(ast, {
      case FilterVariants(
      MatrixRead(hc, path, nPartitions, typ, localValue, dropSamples, _),
      Const(_, false, TBoolean)) =>
        MatrixRead(hc, path, nPartitions, typ, localValue, dropSamples, dropVariants = true)
      case FilterSamples(
      MatrixRead(hc, path, nPartitions, typ, localValue, _, dropVariants),
      Const(_, false, TBoolean)) =>
        MatrixRead(hc, path, nPartitions, typ, localValue, dropSamples = true, dropVariants)

      case FilterVariants(m, Const(_, true, TBoolean)) =>
        m
      case FilterSamples(m, Const(_, true, TBoolean)) =>
        m

      // minor, but push FilterVariants into FilterSamples
      case FilterVariants(FilterSamples(m, spred), vpred) =>
        FilterSamples(FilterVariants(m, vpred), spred)

      case FilterVariants(FilterVariants(m, pred1), pred2) =>
        FilterVariants(m, Apply(pred1.getPos, "&&", Array(pred1, pred2)))

      case FilterSamples(FilterSamples(m, pred1), pred2) =>
        FilterSamples(m, Apply(pred1.getPos, "&&", Array(pred1, pred2)))
    })
  }
}

abstract sealed class MatrixIR extends BaseIR {
  def typ: MatrixType

  def execute(hc: HailContext): MatrixValue
}

case class MatrixLiteral(
  value: MatrixValue) extends MatrixIR {
  def typ: MatrixType = value.typ

  def children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def execute(hc: HailContext): MatrixValue = value

  def copy(newChildren: IndexedSeq[BaseIR]): MatrixLiteral = {
    assert(newChildren.isEmpty)
    this
  }

  override def toString: String = "MatrixLiteral(...)"
}

case class MatrixRead(
  hc: HailContext,
  path: String,
  nPartitions: Int,
  typ: MatrixType,
  localValue: VSMLocalValue,
  dropSamples: Boolean,
  dropVariants: Boolean) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): MatrixRead = {
    assert(newChildren.isEmpty)
    this
  }

  def execute(hc: HailContext): MatrixValue = {
    val rdd =
      if (dropVariants)
        OrderedRDD2.empty(hc.sc, typ.orderedRDD2Type)
      else {
        var rdd = OrderedRDD2(
          typ.orderedRDD2Type,
          OrderedPartitioner2(hc.sc,
            hc.hadoopConf.readFile(path + "/partitioner.json.gz")(JsonMethods.parse(_))),
          new ReadRowsRDD(hc.sc, path, typ.rowType, nPartitions))
        if (dropSamples) {
          val localRowType = typ.rowType
          rdd = rdd.mapPartitionsPreservesPartitioning { it =>
            var rv2b = new RegionValueBuilder()
            var rv2 = RegionValue()

            it.map { rv =>
              rv2b.set(rv.region)

              rv2b.start(localRowType)
              rv2b.startStruct()

              rv2b.addField(localRowType, rv, 0)
              rv2b.addField(localRowType, rv, 1)
              rv2b.addField(localRowType, rv, 2)

              rv2b.startArray(0) // gs
              rv2b.endArray()

              rv2b.endStruct()
              rv2.set(rv.region, rv2b.end())

              rv2
            }
          }
        }

        rdd
      }

    MatrixValue(
      typ,
      if (dropSamples)
        localValue.dropSamples()
      else
        localValue,
      rdd)
  }

  override def toString: String = s"MatrixRead($path, dropSamples = $dropSamples, dropVariants = $dropVariants)"
}

case class FilterSamples(
  child: MatrixIR,
  pred: AST) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array(child)

  def copy(newChildren: IndexedSeq[BaseIR]): FilterSamples = {
    assert(newChildren.length == 1)
    FilterSamples(newChildren(0).asInstanceOf[MatrixIR], pred)
  }

  def typ: MatrixType = child.typ

  def execute(hc: HailContext): MatrixValue = {
    val prev = child.execute(hc)

    val localGlobalAnnotation = prev.localValue.globalAnnotation
    val sas = typ.saType
    val ec = typ.sampleEC

    val f: () => java.lang.Boolean = Parser.evalTypedExpr[java.lang.Boolean](pred, ec)

    val sampleAggregationOption = Aggregators.buildSampleAggregations[Annotation, Annotation, Annotation](hc, prev, ec)

    val p = (s: Annotation, sa: Annotation) => {
      sampleAggregationOption.foreach(f => f.apply(s))
      ec.setAll(localGlobalAnnotation, s, sa)
      f() == true
    }
    prev.filterSamples(p)
  }
}

case class FilterVariants(
  child: MatrixIR,
  pred: AST) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array(child)

  def copy(newChildren: IndexedSeq[BaseIR]): FilterVariants = {
    assert(newChildren.length == 1)
    FilterVariants(newChildren(0).asInstanceOf[MatrixIR], pred)
  }

  def typ: MatrixType = child.typ

  def execute(hc: HailContext): MatrixValue = {
    val prev = child.execute(hc)

    val localGlobalAnnotation = prev.localValue.globalAnnotation
    val ec = child.typ.variantEC

    val f: () => java.lang.Boolean = Parser.evalTypedExpr[java.lang.Boolean](pred, ec)

    val aggregatorOption = Aggregators.buildVariantAggregations[Annotation, Annotation, Annotation](
      prev.rdd2.sparkContext, prev.localValue, ec)

    val localPrevRowType = prev.typ.rowType
    val p = (rv: RegionValue) => {
      val ur = new UnsafeRow(localPrevRowType, rv.region.copy(), rv.offset)

      val v = ur.get(1)
      val va = ur.get(2)
      val gs = ur.getAs[IndexedSeq[Annotation]](3)
      aggregatorOption.foreach(f => f(v, va, gs))

      ec.setAll(localGlobalAnnotation, v, va)

      // null => false
      f() == true
    }

    prev.copy(rdd2 = prev.rdd2.filter(p))
  }
}

case class KeyTableType(rowType: TStruct, key: Array[String]) extends BaseType

abstract sealed class KeyTableIR extends BaseIR {
  def typ: KeyTableType

  def execute(hc: HailContext): RDD[RegionValue]
}

case class KeyTableLiteral(
  typ: KeyTableType,
  rdd2: RDD[RegionValue]) extends KeyTableIR {

  def children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): KeyTableLiteral = {
    assert(newChildren.isEmpty)
    this
  }

  def execute(hc: HailContext): RDD[RegionValue] = rdd2
}
