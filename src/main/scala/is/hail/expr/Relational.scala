package is.hail.expr

import is.hail.HailContext
import is.hail.annotations._
import is.hail.annotations.aggregators.RegionValueAggregator
import is.hail.expr.ir._
import is.hail.expr.types._
import is.hail.io.CodecSpec
import is.hail.methods.Aggregators
import is.hail.rvd._
import is.hail.table.TableSpec
import is.hail.variant._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import is.hail.utils._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.json4s.jackson.JsonMethods

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

  def rewriteTopDown(ast: TableIR, rule: PartialFunction[BaseIR, BaseIR]): TableIR =
    genericRewriteTopDown(ast, rule).asInstanceOf[TableIR]

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

  def rewriteBottomUp(ast: TableIR, rule: PartialFunction[BaseIR, BaseIR]): TableIR =
    genericRewriteBottomUp(ast, rule).asInstanceOf[TableIR]
}

abstract class BaseIR {
  def typ: BaseType

  def children: IndexedSeq[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): BaseIR

  def mapChildren(f: (BaseIR) => BaseIR): BaseIR = {
    copy(children.map(f))
  }
}

case class MatrixValue(
  typ: MatrixType,
  globals: BroadcastRow,
  colValues: BroadcastIndexedSeq,
  rvd: OrderedRVD) {

  assert(rvd.typ == typ.orvdType)

  def sparkContext: SparkContext = rvd.sparkContext

  def nPartitions: Int = rvd.partitions.length

  def nCols: Int = colValues.value.length

  def sampleIds: IndexedSeq[Row] = {
    val queriers = typ.colKey.map(field => typ.colType.query(field))
    colValues.value.map(a => Row.fromSeq(queriers.map(_ (a))))
  }
}

object MatrixIR {
  def optimize(ast: MatrixIR): MatrixIR = {
    BaseIR.rewriteTopDown(ast, {
      case FilterRows(
      MatrixRead(path, spec, dropSamples, _),
      Const(_, false, TBoolean(_))) =>
        MatrixRead(path, spec, dropSamples, dropRows = true)

      case FilterCols(
      MatrixRead(path, spec, _, dropVariants),
      Const(_, false, TBoolean(_))) =>
        MatrixRead(path, spec, dropCols = true, dropVariants)

      case FilterRows(m, Const(_, true, TBoolean(_))) =>
        m
      case FilterCols(m, Const(_, true, TBoolean(_))) =>
        m

      // minor, but push FilterVariants into FilterSamples
      case FilterRows(FilterCols(m, spred), vpred) =>
        FilterCols(FilterRows(m, vpred), spred)

      case FilterRows(FilterRows(m, pred1), pred2) =>
        FilterRows(m, Apply(pred1.getPos, "&&", Array(pred1, pred2)))

      case FilterCols(FilterCols(m, pred1), pred2) =>
        FilterCols(m, Apply(pred1.getPos, "&&", Array(pred1, pred2)))

      // Equivalent rewrites for the new Filter{Cols,Rows}IR
      case FilterRowsIR(MatrixRead(path, spec, dropSamples, _), False()) =>
        MatrixRead(path, spec, dropSamples, dropRows = true)

      case FilterColsIR(MatrixRead(path, spec, dropVariants, _), False()) =>
        MatrixRead(path, spec, dropCols = true, dropVariants)

      // Keep all rows/cols = do nothing
      case FilterRowsIR(m, True()) => m

      case FilterColsIR(m, True()) => m

      // Push FilterRowsIR into FilterColsIR
      case FilterRowsIR(FilterColsIR(m, colPred), rowPred) =>
        FilterColsIR(FilterRowsIR(m, rowPred), colPred)

      // Combine multiple filters into one
      /*
       * FIXME: optimizations disabled due to lack of DoubleAmpersand()
       *
      case FilterRowsIR(FilterRowsIR(m, pred1), pred2) =>
        FilterRowsIR(m,
          ApplyBinaryPrimOp(DoubleAmpersand(), pred1, pred2))

      case FilterColsIR(FilterColsIR(m, pred1), pred2) =>
        FilterColsIR(m,
          ApplyBinaryPrimOp(DoubleAmpersand(), pred1, pred2))
       */
    })
  }

  def chooseColsWithArray(typ: MatrixType): (MatrixType, (MatrixValue, Array[Int]) => MatrixValue) = {
    val rowType = typ.rvRowType
    val keepType = TArray(+TInt32())
    val (rTyp, makeF) = ir.Compile[Long, Long, Long]("row", rowType,
      "keep", keepType,
      body = InsertFields(ir.Ref("row"), Seq((MatrixType.entriesIdentifier,
        ir.ArrayMap(ir.Ref("keep"), "i",
          ir.ArrayRef(ir.GetField(ir.In(0, rowType), MatrixType.entriesIdentifier),
            ir.Ref("i")))))))
    assert(rTyp.isOfType(rowType))

    val newMatrixType = typ.copy(rvRowType = coerce[TStruct](rTyp))

    val keepF = { (mv: MatrixValue, keep: Array[Int]) =>
      val keepBc = mv.sparkContext.broadcast(keep)
      mv.copy(typ = newMatrixType,
        colValues = mv.colValues.copy(value = keep.map(mv.colValues.value)),
        rvd = mv.rvd.mapPartitionsPreservesPartitioning(newMatrixType.orvdType) { it =>
          val f = makeF()
          val keep = keepBc.value
          var rv2 = RegionValue()

          it.map { rv =>
            val region = rv.region
            rv2.set(region,
              f(region, rv.offset, false, region.appendArrayInt(keep), false))
            rv2
          }
        })
    }
    (newMatrixType, keepF)
  }

  def filterCols(typ: MatrixType): (MatrixType, (MatrixValue, (Annotation, Int) => Boolean) => MatrixValue) = {
    val (t, keepF) = chooseColsWithArray(typ)
    (t, {(mv: MatrixValue, p :(Annotation, Int) => Boolean) =>
      val keep = (0 until mv.nCols)
        .view
        .filter { i => p(mv.colValues.value(i), i) }
        .toArray
      keepF(mv, keep)
    })
  }
}

abstract sealed class MatrixIR extends BaseIR {
  def typ: MatrixType

  def partitionCounts: Option[Array[Long]] = None

  def execute(hc: HailContext): MatrixValue
}

case class MatrixLiteral(
  typ: MatrixType,
  value: MatrixValue) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def execute(hc: HailContext): MatrixValue = value

  def copy(newChildren: IndexedSeq[BaseIR]): MatrixLiteral = {
    assert(newChildren.isEmpty)
    this
  }

  override def toString: String = "MatrixLiteral(...)"
}

case class MatrixRead(
  path: String,
  spec: MatrixTableSpec,
  dropCols: Boolean,
  dropRows: Boolean) extends MatrixIR {
  def typ: MatrixType = spec.matrix_type

  override def partitionCounts: Option[Array[Long]] = Some(spec.partitionCounts)

  def children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): MatrixRead = {
    assert(newChildren.isEmpty)
    this
  }

  def execute(hc: HailContext): MatrixValue = {
    val hConf = hc.hadoopConf

    val globals = Annotation.copy(typ.globalType, spec.globalsComponent.readLocal(hc, path)(0)).asInstanceOf[Row]

    val colAnnotations =
      if (dropCols)
        IndexedSeq.empty[Annotation]
      else
        Annotation.copy(TArray(typ.colType), spec.colsComponent.readLocal(hc, path)).asInstanceOf[IndexedSeq[Annotation]]

    val rvd =
      if (dropRows)
        OrderedRVD.empty(hc.sc, typ.orvdType)
      else {
        val fullRowType = typ.rvRowType
        val localEntriesIndex = typ.entriesIdx

        val rowsRVD = spec.rowsComponent.read(hc, path).asInstanceOf[OrderedRVD]
        if (dropCols) {
          rowsRVD.mapPartitionsPreservesPartitioning(typ.orvdType) { it =>
            var rv2b = new RegionValueBuilder()
            var rv2 = RegionValue()

            it.map { rv =>
              rv2b.set(rv.region)
              rv2b.start(fullRowType)
              rv2b.startStruct()
              var i = 0
              while (i < localEntriesIndex) {
                rv2b.addField(fullRowType, rv, i)
                i += 1
              }
              rv2b.startArray(0)
              rv2b.endArray()
              i += 1
              while (i < fullRowType.size) {
                rv2b.addField(fullRowType, rv, i - 1)
                i += 1
              }
              rv2b.endStruct()
              rv2.set(rv.region, rv2b.end())
              rv2
            }
          }
        } else {
          val entriesRVD = spec.entriesComponent.read(hc, path)
          val entriesRowType = entriesRVD.rowType
          rowsRVD.zipPartitionsPreservesPartitioning(
            typ.orvdType,
            entriesRVD
          ) { case (it1, it2) =>
            val rvb = new RegionValueBuilder()

            new Iterator[RegionValue] {
              def hasNext: Boolean = {
                val hn = it1.hasNext
                assert(hn == it2.hasNext)
                hn
              }

              def next(): RegionValue = {
                val rv1 = it1.next()
                val rv2 = it2.next()
                val region = rv2.region
                rvb.set(region)
                rvb.start(fullRowType)
                rvb.startStruct()
                var i = 0
                while (i < localEntriesIndex) {
                  rvb.addField(fullRowType, rv1, i)
                  i += 1
                }
                rvb.addField(entriesRowType, rv2, 0)
                i += 1
                while (i < fullRowType.size) {
                  rvb.addField(fullRowType, rv1, i - 1)
                  i += 1
                }
                rvb.endStruct()
                rv2.set(region, rvb.end())
                rv2
              }
            }
          }
        }
      }

    MatrixValue(
      typ,
      BroadcastRow(globals, typ.globalType, hc.sc),
      BroadcastIndexedSeq(colAnnotations, TArray(typ.colType), hc.sc),
      rvd)
  }

  override def toString: String = s"MatrixRead($path, dropSamples = $dropCols, dropVariants = $dropRows)"
}

case class FilterCols(
  child: MatrixIR,
  pred: AST) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array(child)

  def copy(newChildren: IndexedSeq[BaseIR]): FilterCols = {
    assert(newChildren.length == 1)
    FilterCols(newChildren(0).asInstanceOf[MatrixIR], pred)
  }

  val (typ, filterF) = MatrixIR.filterCols(child.typ)

  def execute(hc: HailContext): MatrixValue = {
    val prev = child.execute(hc)

    val localGlobals = prev.globals.value
    val sas = typ.colType
    val ec = typ.colEC

    val f: () => java.lang.Boolean = Parser.evalTypedExpr[java.lang.Boolean](pred, ec)

    val sampleAggregationOption = Aggregators.buildColAggregations(hc, prev, ec)

    val p = (sa: Annotation, i: Int) => {
      sampleAggregationOption.foreach(f => f.apply(i))
      ec.setAll(localGlobals, sa)
      f() == true
    }
    filterF(prev, p)
  }
}

case class FilterColsIR(
  child: MatrixIR,
  pred: IR) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array(child, pred)

  def copy(newChildren: IndexedSeq[BaseIR]): FilterColsIR = {
    assert(newChildren.length == 2)
    FilterColsIR(newChildren(0).asInstanceOf[MatrixIR], newChildren(1).asInstanceOf[IR])
  }

  val (typ, filterF) = MatrixIR.filterCols(child.typ)

  def execute(hc: HailContext): MatrixValue = {
    val prev = child.execute(hc)

    val localGlobals = prev.globals.broadcast
    val localColType = typ.colType

    //
    // Initialize a region containing the globals
    //
    val colRegion = Region()
    val rvb = new RegionValueBuilder(colRegion)
    rvb.start(typ.globalType)
    rvb.addAnnotation(typ.globalType, localGlobals.value)
    val globalRVend = rvb.currentOffset()
    val globalRVoffset = rvb.end()

    val (rTyp, predCompiledFunc) = ir.Compile[Long, Long, Boolean](
      "global", typ.globalType,
      "sa", typ.colType,
      pred
    )
    // Note that we don't yet support IR aggregators
    val p = (sa: Annotation, i: Int) => {
      colRegion.clear(globalRVend)
      val colRVb = new RegionValueBuilder(colRegion)
      colRVb.start(localColType)
      colRVb.addAnnotation(localColType, sa)
      val colRVoffset = colRVb.end()
      predCompiledFunc()(colRegion, globalRVoffset, false, colRVoffset, false)
    }
    filterF(prev, p)
  }
}

case class FilterRows(
  child: MatrixIR,
  pred: AST) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array(child)

  def copy(newChildren: IndexedSeq[BaseIR]): FilterRows = {
    assert(newChildren.length == 1)
    FilterRows(newChildren(0).asInstanceOf[MatrixIR], pred)
  }

  def typ: MatrixType = child.typ

  def execute(hc: HailContext): MatrixValue = {
    val prev = child.execute(hc)

    val ec = prev.typ.rowEC

    val f: () => java.lang.Boolean = Parser.evalTypedExpr[java.lang.Boolean](pred, ec)

    val aggregatorOption = Aggregators.buildRowAggregations(
      prev.rvd.sparkContext, prev.typ, prev.globals, prev.colValues, ec)

    val fullRowType = prev.typ.rvRowType
    val localRowType = prev.typ.rowType
    val localEntriesIndex = prev.typ.entriesIdx

    val localGlobals = prev.globals.broadcast

    val filteredRDD = prev.rvd.mapPartitionsPreservesPartitioning(prev.typ.orvdType) { it =>
      val fullRow = new UnsafeRow(fullRowType)
      val row = fullRow.deleteField(localEntriesIndex)
      it.filter { rv =>
        fullRow.set(rv)
        ec.set(0, localGlobals.value)
        ec.set(1, row)
        aggregatorOption.foreach(_ (rv))
        f() == true
      }
    }

    prev.copy(rvd = filteredRDD)
  }
}

case class FilterRowsIR(
  child: MatrixIR,
  pred: IR) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array(child, pred)

  def copy(newChildren: IndexedSeq[BaseIR]): FilterRowsIR = {
    assert(newChildren.length == 2)
    FilterRowsIR(newChildren(0).asInstanceOf[MatrixIR], newChildren(1).asInstanceOf[IR])
  }

  def typ: MatrixType = child.typ

  def execute(hc: HailContext): MatrixValue = {
    val prev = child.execute(hc)

    val (rTyp, predCompiledFunc) = ir.Compile[Long, Long, Boolean](
      "va", typ.rvRowType,
      "global", typ.globalType,
      pred
    )

    // Note that we don't yet support IR aggregators
    //
    // Everything used inside the Spark iteration must be serializable,
    // so we pick out precisely what is needed.
    //
    val fullRowType = prev.typ.rvRowType
    val localRowType = prev.typ.rowType
    val localEntriesIndex = prev.typ.entriesIdx
    val localGlobalType = typ.globalType
    val localGlobals = prev.globals.broadcast

    val filteredRDD = prev.rvd.mapPartitionsPreservesPartitioning(prev.typ.orvdType) { it =>
      it.filter { rv =>
        // Append all the globals into this region
        val globalRVb = new RegionValueBuilder(rv.region)
        globalRVb.start(localGlobalType)
        globalRVb.addAnnotation(localGlobalType, localGlobals.value)
        val globalRVoffset = globalRVb.end()
        predCompiledFunc()(rv.region, rv.offset, false, globalRVoffset, false)
      }
    }

    prev.copy(rvd = filteredRDD)
  }
}

case class ChooseCols(child: MatrixIR, oldIndices: Array[Int]) extends MatrixIR {
  def children: IndexedSeq[BaseIR] = Array(child)

  def copy(newChildren: IndexedSeq[BaseIR]): ChooseCols = {
    assert(newChildren.length == 1)
    ChooseCols(newChildren(0).asInstanceOf[MatrixIR], oldIndices)
  }

  val (typ, colsF) = MatrixIR.chooseColsWithArray(child.typ)

  def execute(hc: HailContext): MatrixValue = {
    val prev = child.execute(hc)

    colsF(prev, oldIndices)
  }
}

case class MapEntries(child: MatrixIR, newEntries: IR) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array(child, newEntries)

  def copy(newChildren: IndexedSeq[BaseIR]): MapEntries = {
    assert(newChildren.length == 2)
    MapEntries(newChildren(0).asInstanceOf[MatrixIR], newChildren(1).asInstanceOf[IR])
  }

  val newRow = {
    val arrayLength = ArrayLen(GetField(Ref("va"), MatrixType.entriesIdentifier))
    val idxEnv = new Env[IR]()
      .bind("g", ArrayRef(GetField(Ref("va"), MatrixType.entriesIdentifier), Ref("i")))
      .bind("sa", ArrayRef(Ref("sa"), Ref("i")))
    val entries = ArrayMap(ArrayRange(I32(0), arrayLength, I32(1)), "i", Subst(newEntries, idxEnv))
    InsertFields(Ref("va"), Seq((MatrixType.entriesIdentifier, entries)))
  }

  val typ: MatrixType = {
    Infer(newRow, None, new Env[Type]()
      .bind("global", child.typ.globalType)
      .bind("va", child.typ.rvRowType)
      .bind("sa", TArray(child.typ.colType))
    )
    child.typ.copy(rvRowType = newRow.typ)
  }

  def execute(hc: HailContext): MatrixValue = {
    val prev = child.execute(hc)

    val localGlobalsType = typ.globalType
    val localColsType = TArray(typ.colType)
    val colValuesBc = prev.colValues.broadcast
    val globalsBc = prev.globals.broadcast

    val (rTyp, f) = ir.Compile[Long, Long, Long, Long](
      "global", localGlobalsType,
      "va", prev.typ.rvRowType,
      "sa", localColsType,
      newRow)
    assert(rTyp == typ.rvRowType)

    val newRVD = prev.rvd.mapPartitionsPreservesPartitioning(typ.orvdType) { it =>
      val rvb = new RegionValueBuilder()
      val newRV = RegionValue()
      val rowF = f()

      it.map { rv =>
        val region = rv.region
        val oldRow = rv.offset

        rvb.set(region)
        rvb.start(localGlobalsType)
        rvb.addAnnotation(localGlobalsType, globalsBc.value)
        val globals = rvb.end()

        rvb.start(localColsType)
        rvb.addAnnotation(localColsType, colValuesBc.value)
        val cols = rvb.end()

        val off = rowF(region, globals, false, oldRow, false, cols, false)

        newRV.set(region, off)
        newRV
      }
    }
    prev.copy(typ = typ, rvd = newRVD)
  }
}

case class MapRows(child: MatrixIR, newRow: IR) extends MatrixIR {

  def children: IndexedSeq[BaseIR] = Array(child, newRow)

  def copy(newChildren: IndexedSeq[BaseIR]): MapEntries = {
    assert(newChildren.length == 2)
    MapEntries(newChildren(0).asInstanceOf[MatrixIR], newChildren(1).asInstanceOf[IR])
  }

  val newRVRow = InsertFields(newRow, Seq(MatrixType.entriesIdentifier -> GetField(Ref("va"), MatrixType.entriesIdentifier)))

  val tAggElt: Type = child.typ.entryType
  val aggSymTab = Map(
    "global" -> (0, child.typ.globalType),
    "va" -> (1, child.typ.rowType),
    "g" -> (2, child.typ.entryType),
    "sa" -> (3, child.typ.colType))

  val tAgg = TAggregable(tAggElt, aggSymTab)

  val typ: MatrixType = {
    Infer(newRVRow, Some(tAgg), new Env[Type]()
      .bind("global", child.typ.globalType)
      .bind("va", child.typ.rvRowType)
      .bind("AGG", tAgg)
    )
    val newRVRowSet = newRVRow.typ.fieldNames.toSet
    val newRowKey = child.typ.rowKey.filter(newRVRowSet.contains)
    val newPartitionKey = child.typ.rowPartitionKey.filter(newRVRowSet.contains)
    child.typ.copy(rvRowType = newRVRow.typ, rowKey = newRowKey, rowPartitionKey = newPartitionKey)
  }

  val touchesKeys: Boolean = (typ.rowKey != child.typ.rowKey) ||
    (typ.rowPartitionKey != child.typ.rowPartitionKey) || (
    newRow match {
      case MakeStruct(fields, _) =>
        fields.exists { case (name, ir) =>
          typ.rowKey.contains(name) && (
            ir match {
              case GetField(Ref("va", _), n, _) => n != name
              case _ => true
            })
        }
      case InsertFields(Ref("va", _), toIns, _) => toIns.map(_._1).toSet.intersect(typ.rowKey.toSet).nonEmpty
      case _ => true
    })

  def execute(hc: HailContext): MatrixValue = {
    val prev = child.execute(hc)

    val localRowType = prev.typ.rvRowType
    val localEntriesType = prev.typ.entryArrayType
    val entriesIdx = prev.typ.entriesIdx
    val localGlobalsType = typ.globalType
    val localColsType = TArray(typ.colType)
    val localNCols = prev.nCols
    val colValuesBc = prev.colValues.broadcast
    val globalsBc = prev.globals.broadcast

    val (rvAggs, seqOps, aggResultType, f, rTyp) = ir.CompileWithAggregators[Long, Long, Long, Long, Long, Long, Long, Long](
      "AGG", tAgg,
      "global", localGlobalsType,
      "va", prev.typ.rvRowType,
      newRVRow)
    assert(rTyp == typ.rvRowType, s"$rTyp, ${ typ.rvRowType }")

    val mapPartitionF = { it: Iterator[RegionValue] =>
      val rvb = new RegionValueBuilder()
      val newRV = RegionValue()
      val rowF = f()

      it.map { rv =>
        val region = rv.region
        val oldRow = rv.offset

        rvb.set(region)
        rvb.start(localGlobalsType)
        rvb.addAnnotation(localGlobalsType, globalsBc.value)
        val globals = rvb.end()

        rvb.start(localColsType)
        rvb.addAnnotation(localColsType, colValuesBc.value)
        val cols = rvb.end()

        val entries = localRowType.fieldOffset(oldRow, entriesIdx)

        val aggResults = if (seqOps.nonEmpty) {
          var i = 0
          while (i < localNCols) {
            val eMissing = localEntriesType.isElementMissing(region, entries, i)
            val eOff = localEntriesType.elementOffset(entries, localNCols, i)
            val colMissing = localColsType.isElementMissing(region, cols, i)
            val colOff = localColsType.elementOffset(cols, localNCols, i)
            rvAggs.zip(seqOps).foreach { case (rvagg, seqOp) =>
              seqOp()(region, rvagg, oldRow, false, globals, false, oldRow, false, eOff, eMissing, colOff, colMissing)
            }
            i += 1
          }
          rvAggs
        } else
          Array.empty[RegionValueAggregator]

        rvb.start(aggResultType)
        rvb.startStruct()
        aggResults.foreach(_.result(rvb))
        rvb.endStruct()
        val aggResultsOff = rvb.end()

        val off = rowF(region, aggResultsOff, false, globals, false, oldRow, false)

        newRV.set(region, off)
        newRV
      }
    }

    if (touchesKeys) {
      val newRDD = prev.rvd.mapPartitions(mapPartitionF)
      prev.copy(typ = typ,
        rvd = OrderedRVD.coerce(typ.orvdType, newRDD, None, None))
    } else {
      val newRVD = prev.rvd.mapPartitionsPreservesPartitioning(typ.orvdType)(mapPartitionF)
      prev.copy(typ = typ, rvd = newRVD)
    }
  }
}

case class TableValue(typ: TableType, globals: BroadcastRow, rvd: RVD) {
  def rdd: RDD[Row] =
    rvd.toRows

  def filter(p: (RegionValue, RegionValue) => Boolean): TableValue = {
    val globalType = typ.globalType
    val localGlobals = globals.broadcast
    copy(rvd = rvd.mapPartitions(typ.rowType) { it =>
      val globalRV = RegionValue()
      val globalRVb = new RegionValueBuilder()
      it.filter { rv =>
        globalRVb.set(rv.region)
        globalRVb.start(globalType)
        globalRVb.addAnnotation(globalType, localGlobals.value)
        globalRV.set(rv.region, globalRVb.end())
        p(rv, globalRV)
      }
    })
  }

  def write(path: String, overwrite: Boolean, codecSpecJSONStr: String) {
    val hc = HailContext.get

    val codecSpec =
      if (codecSpecJSONStr != null) {
        implicit val formats = RVDSpec.formats
        val codecSpecJSON = JsonMethods.parse(codecSpecJSONStr)
        codecSpecJSON.extract[CodecSpec]
      } else
        CodecSpec.default

    if (overwrite)
      hc.hadoopConf.delete(path, recursive = true)
    else if (hc.hadoopConf.exists(path))
      fatal(s"file already exists: $path")

    hc.hadoopConf.mkDir(path)

    val globalsPath = path + "/globals"
    hc.hadoopConf.mkDir(globalsPath)
    RVD.writeLocalUnpartitioned(hc, globalsPath, typ.globalType, codecSpec, Array(globals.value))

    val partitionCounts = rvd.write(path + "/rows", codecSpec)

    val referencesPath = path + "/references"
    hc.hadoopConf.mkDir(referencesPath)
    ReferenceGenome.exportReferences(hc, referencesPath, typ.rowType)
    ReferenceGenome.exportReferences(hc, referencesPath, typ.globalType)

    val spec = TableSpec(
      FileFormat.version.rep,
      hc.version,
      "references",
      typ,
      Map("globals" -> RVDComponentSpec("globals"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    spec.write(hc, path)

    hc.hadoopConf.writeTextFile(path + "/_SUCCESS")(out => ())
  }
}


object TableIR {
  def optimize(ir: TableIR): TableIR = {
    BaseIR.rewriteTopDown(ir, {
      case TableFilter(x, True()) => x
      case TableFilter(TableRead(path, spec, _), False() | NA(TBoolean(_))) =>
        TableRead(path, spec, true)
    })
  }
}

abstract sealed class TableIR extends BaseIR {
  def typ: TableType

  def partitionCounts: Option[Array[Long]] = None

  def execute(hc: HailContext): TableValue
}

case class TableLiteral(value: TableValue) extends TableIR {
  val typ: TableType = value.typ

  val children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): TableLiteral = {
    assert(newChildren.isEmpty)
    this
  }

  def execute(hc: HailContext): TableValue = value
}

case class TableRead(path: String, spec: TableSpec, dropRows: Boolean) extends TableIR {
  def typ: TableType = spec.table_type

  override def partitionCounts: Option[Array[Long]] = Some(spec.partitionCounts)

  val children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): TableRead = {
    assert(newChildren.isEmpty)
    this
  }

  def execute(hc: HailContext): TableValue = {
    val globals = spec.globalsComponent.readLocal(hc, path)(0)
    TableValue(typ,
      BroadcastRow(globals, typ.globalType, hc.sc),
      if (dropRows)
        UnpartitionedRVD.empty(hc.sc, typ.rowType)
      else
        spec.rowsComponent.read(hc, path))
  }
}

case class TableParallelize(typ: TableType, rows: IndexedSeq[Row], nPartitions: Option[Int] = None) extends TableIR {
  assert(typ.globalType.size == 0)
  val children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): TableParallelize = {
    assert(newChildren.isEmpty)
    this
  }

  def execute(hc: HailContext): TableValue = {
    val rowTyp = typ.rowType
    val rvd = hc.sc.parallelize(rows, nPartitions.getOrElse(hc.sc.defaultParallelism))
      .mapPartitions(_.toRegionValueIterator(rowTyp))
    TableValue(typ, BroadcastRow(Row(), typ.globalType, hc.sc), new UnpartitionedRVD(rowTyp, rvd))
  }
}

case class TableImport(paths: Array[String], typ: TableType, readerOpts: TableReaderOptions) extends TableIR {
  assert(typ.globalType.size == 0)
  val children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): TableImport = {
    assert(newChildren.isEmpty)
    this
  }

  def execute(hc: HailContext): TableValue = {
    val rowTyp = typ.rowType
    val nField = rowTyp.fields.length
    val rowFields = rowTyp.fields

    val rvd = hc.sc.textFilesLines(paths, readerOpts.nPartitions)
      .filter { line =>
        !readerOpts.isComment(line.value) &&
          (readerOpts.noHeader || readerOpts.header != line.value) &&
          !(readerOpts.skipBlankLines && line.value.isEmpty)
      }.mapPartitions { it =>
      val region = Region()
      val rvb = new RegionValueBuilder(region)
      val rv = RegionValue(region)

      it.map {
        _.map { line =>
          region.clear()

          val split = TextTableReader.splitLine(line, readerOpts.separator, readerOpts.quote)
          if (split.length != nField)
            fatal(s"expected $nField fields, but found ${ split.length } fields")
          rvb.set(region)
          rvb.start(rowTyp)
          rvb.startStruct()

          var i = 0
          while (i < nField) {
            val Field(name, t, _) = rowFields(i)
            val field = split(i)
            try {
              if (field == readerOpts.missing)
                rvb.setMissing()
              else
                rvb.addAnnotation(t, TableAnnotationImpex.importAnnotation(field, t))
            } catch {
              case e: Exception =>
                fatal(s"""${ e.getClass.getName }: could not convert "$field" to $t in column "$name" """)
            }
            i += 1
          }

          rvb.endStruct()
          rv.setOffset(rvb.end())
          rv
        }.value
      }
    }

    TableValue(typ, BroadcastRow(Row.empty, typ.globalType, hc.sc), new UnpartitionedRVD(rowTyp, rvd))
  }
}

case class TableFilter(child: TableIR, pred: IR) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array(child, pred)

  val typ: TableType = child.typ

  def copy(newChildren: IndexedSeq[BaseIR]): TableFilter = {
    assert(newChildren.length == 2)
    TableFilter(newChildren(0).asInstanceOf[TableIR], newChildren(1).asInstanceOf[IR])
  }

  def execute(hc: HailContext): TableValue = {
    val ktv = child.execute(hc)
    val (rTyp, f) = ir.Compile[Long, Long, Boolean](
      "row", child.typ.rowType,
      "global", child.typ.globalType,
      pred)
    assert(rTyp == TBoolean())
    ktv.filter((rv, globalRV) => f()(rv.region, rv.offset, false, globalRV.offset, false))
  }
}

case class TableJoin(left: TableIR, right: TableIR, joinType: String) extends TableIR {
  require(left.typ.keyType isIsomorphicTo right.typ.keyType)

  val children: IndexedSeq[BaseIR] = Array(left, right)

  private val joinedFields = left.typ.keyType.fields ++
    left.typ.valueType.fields ++
    right.typ.valueType.fields
  private val preNames = joinedFields.map(_.name).toArray
  private val (finalColumnNames, remapped) = mangle(preNames)

  val newRowType = TStruct(joinedFields.zipWithIndex.map {
    case (fd, i) => (finalColumnNames(i), fd.typ)
  }: _*)

  val typ: TableType = left.typ.copy(rowType = newRowType)

  def copy(newChildren: IndexedSeq[BaseIR]): TableJoin = {
    assert(newChildren.length == 2)
    TableJoin(
      newChildren(0).asInstanceOf[TableIR],
      newChildren(1).asInstanceOf[TableIR],
      joinType)
  }

  def execute(hc: HailContext): TableValue = {
    val leftTV = left.execute(hc)
    val rightTV = right.execute(hc)
    val leftRowType = left.typ.rowType
    val rightRowType = right.typ.rowType
    val leftKeyFieldIdx = left.typ.keyFieldIdx
    val rightKeyFieldIdx = right.typ.keyFieldIdx
    val leftValueFieldIdx = left.typ.valueFieldIdx
    val rightValueFieldIdx = right.typ.valueFieldIdx
    val localNewRowType = newRowType
    val rvMerger: Iterator[JoinedRegionValue] => Iterator[RegionValue] = { it =>
      val rvb = new RegionValueBuilder()
      val rv = RegionValue()
      it.map { joined =>
        val lrv = joined._1
        val rrv = joined._2

        if (lrv != null)
          rvb.set(lrv.region)
        else {
          assert(rrv != null)
          rvb.set(rrv.region)
        }

        rvb.start(localNewRowType)
        rvb.startStruct()

        if (lrv != null)
          rvb.addFields(leftRowType, lrv, leftKeyFieldIdx)
        else {
          assert(rrv != null)
          rvb.addFields(rightRowType, rrv, rightKeyFieldIdx)
        }

        if (lrv != null)
          rvb.addFields(leftRowType, lrv, leftValueFieldIdx)
        else
          rvb.skipFields(leftValueFieldIdx.length)

        if (rrv != null)
          rvb.addFields(rightRowType, rrv, rightValueFieldIdx)
        else
          rvb.skipFields(rightValueFieldIdx.length)

        rvb.endStruct()
        rv.set(rvb.region, rvb.end())
        rv
      }
    }
    val leftORVD = leftTV.rvd match {
      case ordered: OrderedRVD => ordered
      case unordered =>
        OrderedRVD.coerce(
          new OrderedRVDType(left.typ.key.toArray, left.typ.key.toArray, leftRowType),
          unordered.rdd,
          None,
          None)
    }
    val rightORVD = rightTV.rvd match {
      case ordered: OrderedRVD => ordered
      case unordered =>
        val ordType =
          new OrderedRVDType(right.typ.key.toArray, right.typ.key.toArray, rightRowType)
        if (joinType == "left" || joinType == "inner")
          unordered.constrainToOrderedPartitioner(ordType, leftORVD.partitioner)
        else
          OrderedRVD.coerce(ordType, unordered.rdd, None, Some(leftORVD.partitioner))
    }
    val joinedRVD = leftORVD.orderedJoin(
      rightORVD,
      joinType,
      rvMerger,
      new OrderedRVDType(leftORVD.typ.partitionKey, leftORVD.typ.key, newRowType))

    TableValue(typ, leftTV.globals, joinedRVD)
  }
}

case class TableMapRows(child: TableIR, newRow: IR) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array(child, newRow)

  val typ: TableType = {
    Infer(newRow, None, child.typ.env)
    val newRowType = newRow.typ.asInstanceOf[TStruct]
    val newKey = child.typ.key.filter(newRowType.fieldIdx.contains)
    child.typ.copy(rowType = newRowType, key = newKey)
  }

  def copy(newChildren: IndexedSeq[BaseIR]): TableMapRows = {
    assert(newChildren.length == 2)
    TableMapRows(newChildren(0).asInstanceOf[TableIR], newChildren(1).asInstanceOf[IR])
  }

  def execute(hc: HailContext): TableValue = {
    val tv = child.execute(hc)
    val (rTyp, f) = ir.Compile[Long, Long, Long](
      "row", child.typ.rowType,
      "global", child.typ.globalType,
      newRow)
    assert(rTyp == typ.rowType)
    val globalsBc = tv.globals.broadcast
    val gType = typ.globalType
    TableValue(typ,
      tv.globals,
      tv.rvd.mapPartitions(typ.rowType) { it =>
        val globalRV = RegionValue()
        val globalRVb = new RegionValueBuilder()
        val rv2 = RegionValue()
        val newRow = f()
        it.map { rv =>
          globalRVb.set(rv.region)
          globalRVb.start(gType)
          globalRVb.addAnnotation(gType, globalsBc.value)
          globalRV.set(rv.region, globalRVb.end())
          rv2.set(rv.region, newRow(rv.region, rv.offset, false, globalRV.offset, false))
          rv2
        }
      })
  }
}

case class TableMapGlobals(child: TableIR, newRow: IR) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array(child, newRow)

  val typ: TableType = {
    Infer(newRow, None, child.typ.env)
    child.typ.copy(globalType = newRow.typ.asInstanceOf[TStruct])
  }

  def copy(newChildren: IndexedSeq[BaseIR]): TableMapGlobals = {
    assert(newChildren.length == 2)
    TableMapGlobals(newChildren(0).asInstanceOf[TableIR], newChildren(1).asInstanceOf[IR])
  }

  def execute(hc: HailContext): TableValue = {
    val tv = child.execute(hc)
    val gType = typ.globalType

    val (rTyp, f) = ir.Compile[Long, Long](
      "global", child.typ.globalType,
      newRow)
    assert(rTyp == gType)

    val globalRegion = Region()
    val globalOff = tv.globals.toRegion(globalRegion)
    val newOff = f()(globalRegion, globalOff, false)

    val newGlobals = tv.globals.copy(
      value = SafeRow(rTyp.asInstanceOf[TStruct], globalRegion, newOff),
      t = rTyp.asInstanceOf[TStruct])

    TableValue(typ, newGlobals, tv.rvd)
  }
}
