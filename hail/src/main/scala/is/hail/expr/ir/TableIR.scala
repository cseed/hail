package is.hail.expr.ir

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import is.hail.HailContext
import is.hail.annotations._
import is.hail.asm4s._
import is.hail.expr.ir
import is.hail.expr.ir.EmitStream.SizedStream
import is.hail.expr.ir.functions.{BlockMatrixToTableFunction, MatrixToTableFunction, TableToTableFunction}
import is.hail.expr.types._
import is.hail.expr.types.physical._
import is.hail.expr.types.virtual._
import is.hail.io._
import is.hail.io.fs.FS
import is.hail.linalg.{BlockMatrix, BlockMatrixMetadata, BlockMatrixReadRowBlockedRDD}
import is.hail.rvd._
import is.hail.sparkextras.ContextRDD
import is.hail.utils._
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.json4s.JsonAST.{JNothing, JString}
import org.json4s.{Formats, JObject, ShortTypeHints}

import scala.reflect.ClassTag

object TableIR {
  def read(fs: FS, path: String, dropRows: Boolean = false, requestedType: Option[TableType] = None): TableIR = {
    val successFile = path + "/_SUCCESS"
    if (!fs.exists(path + "/_SUCCESS"))
      fatal(s"write failed: file not found: $successFile")

    val tr = TableNativeReader.read(fs, path, None)
    TableRead(requestedType.getOrElse(tr.fullType), dropRows = dropRows, tr)
  }
}

abstract sealed class TableIR extends BaseIR {
  def typ: TableType

  def partitionCounts: Option[IndexedSeq[Long]] = None

  val rowCountUpperBound: Option[Long]

  protected[ir] def execute(ctx: ExecuteContext): TableValue =
    fatal("tried to execute unexecutable IR:\n" + Pretty(this))

  override def copy(newChildren: IndexedSeq[BaseIR]): TableIR

  def persist(storageLevel: StorageLevel): TableIR = {
    // FIXME: store table literal in cache, return ID
    ExecuteContext.scoped() { ctx =>
      val tv = Interpret(this, ctx, optimize = true)
      TableLiteral(tv.persist(storageLevel), ctx)
    }
  }

  def unpersist(): TableIR = {
    this match {
      case TableLiteral(typ, rvd, enc, encodedGlobals) => TableLiteral(typ, rvd.unpersist(), enc, encodedGlobals)
      case x => x
    }
  }

  def pyPersist(storageLevel: String): TableIR = {
    val level = try {
      StorageLevel.fromString(storageLevel)
    } catch {
      case e: IllegalArgumentException =>
        fatal(s"unknown StorageLevel: $storageLevel")
    }
    persist(level)
  }

  def pyUnpersist(): TableIR = unpersist()

  def pyToDF(): DataFrame = {
    ExecuteContext.scoped() { ctx =>
      Interpret(this, ctx).toDF()
    }
  }
}

object TableLiteral {
  def apply(value: TableValue, ctx: ExecuteContext): TableLiteral = {
    val globalPType = PType.canonical(value.globals.t)
    val enc = TypedCodecSpec(globalPType, BufferSpec.wireSpec) // use wireSpec to save memory
    using(new ByteArrayEncoder(enc.buildEncoder(value.globals.t))) { encoder =>
      TableLiteral(value.typ, value.rvd, enc,
        encoder.regionValueToBytes(value.globals.value.offset))
    }
  }
}

case class TableLiteral(typ: TableType, rvd: RVD, enc: AbstractTypedCodecSpec, encodedGlobals: Array[Byte]) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  lazy val rowCountUpperBound: Option[Long] = None

  def copy(newChildren: IndexedSeq[BaseIR]): TableLiteral = {
    assert(newChildren.isEmpty)
    TableLiteral(typ, rvd, enc, encodedGlobals)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val (globalPType: PStruct, dec) = enc.buildDecoder(typ.globalType)

    val bais = new ByteArrayInputStream(encodedGlobals)
    val globalOffset = dec.apply(bais).readRegionValue(ctx.r)
    TableValue(typ, BroadcastRow(RegionValue(ctx.r, globalOffset), globalPType, HailContext.backend), rvd)
  }
}

object TableReader {
  implicit val formats: Formats = RelationalSpec.formats + ShortTypeHints(
    List(classOf[TableNativeReader],
      classOf[TableNativeZippedReader],
      classOf[TextTableReader],
      classOf[TextInputFilterAndReplace],
      classOf[TableFromBlockMatrixNativeReader])
    ) + new NativeReaderOptionsSerializer()

  def fromJson(fs: FS, jv: JObject): TableReader = {
    (jv \ "name").extract[String] match {
      case "TableNativeReader" => TableNativeReader.fromJson(fs, jv)
      case _ => jv.extract[TableReader]
    }
  }
}

abstract class TableReader {
  def pathsUsed: Seq[String]

  def apply(tr: TableRead, ctx: ExecuteContext): TableValue

  def partitionCounts: Option[IndexedSeq[Long]]

  def fullType: TableType
}

object TableNativeReader {
  def read(fs: FS, path: String, options: Option[NativeReaderOptions]): TableNativeReader = {
    val spec = (RelationalSpec.read(fs, path): @unchecked) match {
      case ts: AbstractTableSpec => ts
      case _: AbstractMatrixTableSpec => fatal(s"file is a MatrixTable, not a Table: '$path'")
    }

    TableNativeReader(path, options, spec)
  }

  def fromJson(fs: FS, readerJV: JObject): TableNativeReader = {
    val path = readerJV \ "path" match {
      case JString(s) => s
    }

    val options = readerJV \ "options" match {
      case optionsJV: JObject =>
        Some(NativeReaderOptions.fromJson(optionsJV))
      case JNothing => None
    }

    read(fs, path, options)
  }
}

case class PartitionNativeReader(spec: AbstractTypedCodecSpec, fullRowType: TStruct) extends PartitionReader {
  def contextType: Type = TString

  def rowPType(requestedType: Type): PType = spec.buildDecoder(requestedType)._1

  def emitStream[C](context: IR,
    requestedType: Type,
    emitter: Emit[C],
    mb: EmitMethodBuilder[C],
    region: Value[Region],
    env: Emit.E,
    container: Option[AggContainer]): COption[SizedStream] = {

    def emitIR(ir: IR, env: Emit.E = env, region: Value[Region] = region, container: Option[AggContainer] = container): EmitCode =
      emitter.emitWithRegion(ir, mb, region, env, container)

    val (eltType, dec) = spec.buildEmitDecoderF[Long](requestedType, mb.ecb)

    COption.fromEmitCode(emitIR(context)).map { path =>
      val pathString = path.asString.loadString()
      val xRowBuf = mb.newLocal[InputBuffer]()
      val stream = Stream.unfold[Code[Long]](
        Code._empty,
        Code._empty,
        (_, k) =>
          k(COption(
            !xRowBuf.load().readByte().toZ,
            dec(region, xRowBuf))))
        .map(
          EmitCode.present(eltType, _),
          setup0 = None,
          setup = Some(xRowBuf := spec
            .buildCodeInputBuffer(mb.open(pathString, true))))

      SizedStream.unsized(stream)
    }
  }
}

case class TableNativeReader(
  path: String,
  options: Option[NativeReaderOptions],
  spec: AbstractTableSpec
) extends TableReader {
  def partitionCounts: Option[IndexedSeq[Long]] = if (intervals.isEmpty) Some(spec.partitionCounts) else None
  def pathsUsed: Seq[String] = Array(path)

  override lazy val fullType: TableType = spec.table_type

  private val filterIntervals = options.map(_.filterIntervals).getOrElse(false)
  private def intervals = options.map(_.intervals)

  if (intervals.nonEmpty && !spec.indexed(path))
    fatal("""`intervals` specified on an unindexed table.
            |This table was written using an older version of hail
            |rewrite the table in order to create an index to proceed""".stripMargin)

  def apply(tr: TableRead, ctx: ExecuteContext): TableValue = {
    val hc = HailContext.get

    val (globalType, globalsOffset) = spec.globalsComponent.readLocalSingleRow(hc, path, tr.typ.globalType, ctx.r)
    val rvd = if (tr.dropRows) {
      RVD.empty(hc.sc, tr.typ.canonicalRVDType)
    } else {
      val partitioner = if (filterIntervals)
        intervals.map(i => RVDPartitioner.union(tr.typ.keyType, i, tr.typ.key.length - 1))
      else
        intervals.map(i => new RVDPartitioner(tr.typ.keyType, i))
      val rvd = spec.rowsComponent.read(hc, path, tr.typ.rowType, ctx, partitioner, filterIntervals)
      if (rvd.typ.key startsWith tr.typ.key)
        rvd
      else {
        log.info("Sorting a table after read. Rewrite the table to prevent this in the future.")
        rvd.changeKey(tr.typ.key, ctx)
      }
    }
    TableValue(tr.typ, BroadcastRow(RegionValue(ctx.r, globalsOffset), globalType.setRequired(true).asInstanceOf[PStruct], hc.backend), rvd)
  }
}

case class TableNativeZippedReader(
  pathLeft: String,
  pathRight: String,
  options: Option[NativeReaderOptions],
  specLeft: AbstractTableSpec,
  specRight: AbstractTableSpec
) extends TableReader {
  def pathsUsed: Seq[String] = FastSeq(pathLeft, pathRight)
  private lazy val filterIntervals = options.map(_.filterIntervals).getOrElse(false)
  private def intervals = options.map(_.intervals)

  require((specLeft.table_type.rowType.fieldNames ++ specRight.table_type.rowType.fieldNames).areDistinct())
  require(specRight.table_type.key.isEmpty)
  require(specLeft.partitionCounts sameElements specRight.partitionCounts)
  require(specLeft.version == specRight.version)

  def partitionCounts: Option[IndexedSeq[Long]] = if (intervals.isEmpty) Some(specLeft.partitionCounts) else None

  override lazy val fullType: TableType = specLeft.table_type.copy(rowType = specLeft.table_type.rowType ++ specRight.table_type.rowType)

  def apply(tr: TableRead, ctx: ExecuteContext): TableValue = {
    val hc = HailContext.get
    val (globalPType: PStruct, globalsOffset) = specLeft.globalsComponent.readLocalSingleRow(hc, pathLeft, tr.typ.globalType, ctx.r)
    val rvd = if (tr.dropRows) {
      RVD.empty(hc.sc, tr.typ.canonicalRVDType)
    } else {
      val partitioner = if (filterIntervals)
        intervals.map(i => RVDPartitioner.union(tr.typ.keyType, i, tr.typ.key.length - 1))
      else
        intervals.map(i => new RVDPartitioner(tr.typ.keyType, i))
      val leftFieldSet = specLeft.table_type.rowType.fieldNames.toSet
      val rightFieldSet = specRight.table_type.rowType.fieldNames.toSet
      if (tr.typ.rowType.fieldNames.forall(f => !rightFieldSet.contains(f))) {
        specLeft.rowsComponent.read(hc, pathLeft, tr.typ.rowType, ctx, partitioner, filterIntervals)
      } else if (tr.typ.rowType.fieldNames.forall(f => !leftFieldSet.contains(f))) {
        specRight.rowsComponent.read(hc, pathRight, tr.typ.rowType, ctx, partitioner, filterIntervals)
      } else {
        val rvdSpecLeft = specLeft.rowsComponent.rvdSpec(hc.fs, pathLeft)
        val rvdSpecRight = specRight.rowsComponent.rvdSpec(hc.fs, pathRight)
        val rvdPathLeft = specLeft.rowsComponent.absolutePath(pathLeft)
        val rvdPathRight = specRight.rowsComponent.absolutePath(pathRight)

        val leftRType = tr.typ.rowType.filter(f => leftFieldSet.contains(f.name))._1
        val rightRType = tr.typ.rowType.filter(f => rightFieldSet.contains(f.name))._1
        AbstractRVDSpec.readZipped(hc,
          rvdSpecLeft, rvdSpecRight,
          rvdPathLeft, rvdPathRight,
          tr.typ.rowType,
          leftRType, rightRType,
          partitioner, filterIntervals,
          ctx)
      }
    }

    TableValue(tr.typ, BroadcastRow(RegionValue(ctx.r, globalsOffset), globalPType.setRequired(true).asInstanceOf[PStruct], hc.backend), rvd)
  }
}

case class TableFromBlockMatrixNativeReader(path: String, nPartitions: Option[Int] = None) extends TableReader {
  def pathsUsed: Seq[String] = FastSeq(path)
  val metadata: BlockMatrixMetadata = BlockMatrix.readMetadata(HailContext.get, path)
  val getNumPartitions: Int = nPartitions.getOrElse(HailContext.get.sc.defaultMinPartitions)

  val partitionRanges = (0 until getNumPartitions).map { i =>
    val nRows = metadata.nRows
    val start = (i * nRows) / getNumPartitions
    val end = ((i + 1) * nRows) / getNumPartitions
    start until end
  }

  override def partitionCounts: Option[IndexedSeq[Long]] = {
    Some(partitionRanges.map(r => r.end - r.start))
  }

  override lazy val fullType: TableType = {
    val rowType = TStruct("row_idx" -> TInt64, "entries" -> TArray(TFloat64))
    TableType(rowType, Array("row_idx"), TStruct.empty)
  }

  def apply(tr: TableRead, ctx: ExecuteContext): TableValue = {
    val rowsRDD = new BlockMatrixReadRowBlockedRDD(path, partitionRanges, metadata, HailContext.get)

    val partitionBounds = partitionRanges.map { r => Interval(Row(r.start), Row(r.end), true, false) }
    val partitioner = new RVDPartitioner(fullType.keyType, partitionBounds)

    val rvd = RVD(fullType.canonicalRVDType, partitioner, ContextRDD(rowsRDD))
    TableValue(fullType, BroadcastRow.empty(ctx), rvd)
  }
}

case class TableRead(typ: TableType, dropRows: Boolean, tr: TableReader) extends TableIR {
  assert(PruneDeadFields.isSupertype(typ, tr.fullType),
    s"\n  original:  ${ tr.fullType }\n  requested: $typ")

  override def partitionCounts: Option[IndexedSeq[Long]] = if (dropRows) Some(FastIndexedSeq(0L)) else tr.partitionCounts

  lazy val rowCountUpperBound: Option[Long] = partitionCounts.map(_.sum)

  val children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): TableRead = {
    assert(newChildren.isEmpty)
    TableRead(typ, dropRows, tr)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = tr.apply(this, ctx)
}

case class TableParallelize(rowsAndGlobal: IR, nPartitions: Option[Int] = None) extends TableIR {
  require(rowsAndGlobal.typ.isInstanceOf[TStruct])
  require(rowsAndGlobal.typ.asInstanceOf[TStruct].fieldNames.sameElements(Array("rows", "global")))

  lazy val rowCountUpperBound: Option[Long] = None

  private val rowsType = rowsAndGlobal.typ.asInstanceOf[TStruct].fieldType("rows").asInstanceOf[TArray]
  private val globalsType = rowsAndGlobal.typ.asInstanceOf[TStruct].fieldType("global").asInstanceOf[TStruct]

  val children: IndexedSeq[BaseIR] = FastIndexedSeq(rowsAndGlobal)

  def copy(newChildren: IndexedSeq[BaseIR]): TableParallelize = {
    val IndexedSeq(newrowsAndGlobal: IR) = newChildren
    TableParallelize(newrowsAndGlobal, nPartitions)
  }

  val typ: TableType = TableType(
    rowsType.elementType.asInstanceOf[TStruct],
    FastIndexedSeq(),
    globalsType)

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val hc = HailContext.get
    val (ptype: PStruct, res) = CompileAndEvaluate._apply(ctx, rowsAndGlobal, optimize = false) match {
      case Right((t: PTuple, off)) => (t.fields(0).typ, t.loadField(off, 0))
    }

    val globalsT = ptype.types(1).asInstanceOf[PStruct]
    val globals = BroadcastRow(RegionValue(ctx.r, ptype.loadField(res, 1)), globalsT, ctx.backend)

    val rowsT = ptype.types(0).asInstanceOf[PArray]
    val rowT = rowsT.elementType.asInstanceOf[PStruct].setRequired(true)
    val spec = TypedCodecSpec(rowT, BufferSpec.wireSpec)

    val makeEnc = spec.buildEncoder(rowT)
    val rowsAddr = ptype.loadField(res, 0)
    val nRows = rowsT.loadLength(rowsAddr)

    val nSplits = math.min(nPartitions.getOrElse(16), math.max(nRows, 1))
    val parts = partition(nRows, nSplits)

    val bae = new ByteArrayEncoder(makeEnc)
    var idx = 0
    val encRows = Array.tabulate(nSplits) { splitIdx =>
      val n = parts(splitIdx)
      bae.reset()
      val stop = idx + n
      while (idx < stop) {
        if (rowsT.isElementMissing(rowsAddr, idx))
          fatal(s"cannot parallelize null values: found null value at index $idx")
        bae.writeRegionValue(ctx.r, rowsT.loadElement(rowsAddr, idx))
        idx += 1
      }
      (n, bae.result())
    }

    val (resultRowType: PStruct, makeDec) = spec.buildDecoder(typ.rowType)
    assert(resultRowType.virtualType == typ.rowType)

    log.info(s"parallelized $nRows rows in $nSplits partitions")

    val rvd = ContextRDD.parallelize(hc.sc, encRows, encRows.length)
      .cmapPartitions { (ctx, it) =>
        it.flatMap { case (nRowPartition, arr) =>
          val bais = new ByteArrayDecoder(makeDec)
          bais.set(arr)
          Iterator.range(0, nRowPartition)
            .map { _ =>
              bais.readValue(ctx.region)
           }
        }
      }
    TableValue(typ, globals, RVD.unkeyed(resultRowType, rvd))
  }
}

/**
  * Change the table to have key 'keys'.
  *
  * Let n be the longest common prefix of 'keys' and the old key, i.e. the
  * number of key fields that are not being changed.
  * - If 'isSorted', then 'child' must already be sorted by 'keys', and n must
  * not be zero. Thus, if 'isSorted', TableKeyBy will not shuffle or scan.
  * The new partitioner will be the old one with partition bounds truncated
  * to length n.
  * - If n = 'keys.length', i.e. we are simply shortening the key, do nothing
  * but change the table type to the new key. 'isSorted' is ignored.
  * - Otherwise, if 'isSorted' is false and n < 'keys.length', then shuffle.
  */
case class TableKeyBy(child: TableIR, keys: IndexedSeq[String], isSorted: Boolean = false) extends TableIR {
  private val fields = child.typ.rowType.fieldNames.toSet
  assert(keys.forall(fields.contains), s"${ keys.filter(k => !fields.contains(k)).mkString(", ") }")

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  val children: IndexedSeq[BaseIR] = Array(child)

  val typ: TableType = child.typ.copy(key = keys)

  def definitelyDoesNotShuffle: Boolean = child.typ.key.startsWith(keys) || isSorted

  def copy(newChildren: IndexedSeq[BaseIR]): TableKeyBy = {
    assert(newChildren.length == 1)
    TableKeyBy(newChildren(0).asInstanceOf[TableIR], keys, isSorted)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val tv = child.execute(ctx)
    tv.copy(typ = typ, rvd = tv.rvd.enforceKey(keys, ctx, isSorted))
  }
}

case class TableRange(n: Int, nPartitions: Int) extends TableIR {
  require(n >= 0)
  require(nPartitions > 0)
  private val nPartitionsAdj = math.max(math.min(n, nPartitions), 1)
  val children: IndexedSeq[BaseIR] = Array.empty[BaseIR]

  def copy(newChildren: IndexedSeq[BaseIR]): TableRange = {
    assert(newChildren.isEmpty)
    TableRange(n, nPartitions)
  }

  private val partCounts = partition(n, nPartitionsAdj)

  override val partitionCounts = Some(partCounts.map(_.toLong).toFastIndexedSeq)

  lazy val rowCountUpperBound: Option[Long] = Some(n.toLong)

  val typ: TableType = TableType(
    TStruct("idx" -> TInt32),
    Array("idx"),
    TStruct.empty)

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val localRowType = PCanonicalStruct(true, "idx" -> PInt32Required)
    val localPartCounts = partCounts
    val partStarts = partCounts.scanLeft(0)(_ + _)
    val hc = HailContext.get
    TableValue(typ,
      BroadcastRow.empty(ctx),
      new RVD(
        RVDType(localRowType, Array("idx")),
        new RVDPartitioner(Array("idx"), typ.rowType,
          Array.tabulate(nPartitionsAdj) { i =>
            val start = partStarts(i)
            val end = partStarts(i + 1)
            Interval(Row(start), Row(end), includesStart = true, includesEnd = false)
          }),
        ContextRDD.parallelize(hc.sc, Range(0, nPartitionsAdj), nPartitionsAdj)
          .cmapPartitionsWithIndex { case (i, ctx, _) =>
            val region = ctx.region

            val start = partStarts(i)
            Iterator.range(start, start + localPartCounts(i))
              .map { j =>
                val off = localRowType.allocate(region)
                localRowType.setFieldPresent(off, 0)
                Region.storeInt(localRowType.fieldOffset(off, 0), j)
                off
              }
          }))
  }
}

case class TableFilter(child: TableIR, pred: IR) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array(child, pred)

  val typ: TableType = child.typ

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  def copy(newChildren: IndexedSeq[BaseIR]): TableFilter = {
    assert(newChildren.length == 2)
    TableFilter(newChildren(0).asInstanceOf[TableIR], newChildren(1).asInstanceOf[IR])
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val tv = child.execute(ctx)

    if (pred == True())
      return tv
    else if (pred == False())
      return tv.copy(rvd = RVD.empty(HailContext.get.sc, typ.canonicalRVDType))

    val (rTyp, f) = ir.Compile[AsmFunction3RegionLongLongBoolean](
      ctx,
      FastIndexedSeq(("row", tv.rvd.rowPType),
        ("global", tv.globals.t)),
      FastIndexedSeq(classInfo[Region], LongInfo, LongInfo), BooleanInfo,
      Coalesce(FastIndexedSeq(pred, False())))
    assert(rTyp.virtualType == TBoolean)

    tv.filterWithPartitionOp(f)((rowF, ctx, ptr, globalPtr) => rowF(ctx.region, ptr, globalPtr))
  }
}

object TableSubset {
  val HEAD: Int = 0
  val TAIL: Int = 1
}

trait TableSubset extends TableIR {
  val subsetKind: Int
  val child: TableIR
  val n: Long

  def typ: TableType = child.typ

  lazy val children: IndexedSeq[BaseIR] = FastIndexedSeq(child)

  override def partitionCounts: Option[IndexedSeq[Long]] =
    child.partitionCounts.map(subsetKind match {
      case TableSubset.HEAD => PartitionCounts.getHeadPCs(_, n)
      case TableSubset.TAIL => PartitionCounts.getTailPCs(_, n)
    })

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound match {
    case Some(c) => Some(c.min(n))
    case None => Some(n)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val prev = child.execute(ctx)
    prev.copy(rvd = subsetKind match {
      case TableSubset.HEAD => prev.rvd.head(n, child.partitionCounts)
      case TableSubset.TAIL => prev.rvd.tail(n, child.partitionCounts)
    })
  }
}

case class TableHead(child: TableIR, n: Long) extends TableSubset {
  require(n >= 0, fatal(s"TableHead: n must be non-negative! Found '$n'."))
  val subsetKind = TableSubset.HEAD

  def copy(newChildren: IndexedSeq[BaseIR]): TableHead = {
    val IndexedSeq(newChild: TableIR) = newChildren
    TableHead(newChild, n)
  }
}

case class TableTail(child: TableIR, n: Long) extends TableSubset {
  require(n >= 0, fatal(s"TableTail: n must be non-negative! Found '$n'."))
  val subsetKind = TableSubset.TAIL

  def copy(newChildren: IndexedSeq[BaseIR]): TableTail = {
    val IndexedSeq(newChild: TableIR) = newChildren
    TableTail(newChild, n)
  }
}

object RepartitionStrategy {
  val SHUFFLE: Int = 0
  val COALESCE: Int = 1
  val NAIVE_COALESCE: Int = 2
}

case class TableRepartition(child: TableIR, n: Int, strategy: Int) extends TableIR {
  def typ: TableType = child.typ

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  lazy val children: IndexedSeq[BaseIR] = FastIndexedSeq(child)

  def copy(newChildren: IndexedSeq[BaseIR]): TableRepartition = {
    val IndexedSeq(newChild: TableIR) = newChildren
    TableRepartition(newChild, n, strategy)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val prev = child.execute(ctx)
    val rvd = strategy match {
      case RepartitionStrategy.SHUFFLE => prev.rvd.coalesce(n, ctx, shuffle = true)
      case RepartitionStrategy.COALESCE => prev.rvd.coalesce(n, ctx, shuffle = false)
      case RepartitionStrategy.NAIVE_COALESCE => prev.rvd.naiveCoalesce(n, ctx)
    }

    prev.copy(rvd = rvd)
  }
}

object TableJoin {
  def apply(left: TableIR, right: TableIR, joinType: String): TableJoin =
    TableJoin(left, right, joinType, left.typ.key.length)
}

/**
  * Suppose 'left' has key [l_1, ..., l_n] and 'right' has key [r_1, ..., r_m].
  * Then [l_1, ..., l_j] and [r_1, ..., r_j] must have the same type, where
  * j = 'joinKey'. TableJoin computes the join of 'left' and 'right' along this
  * common prefix of their keys, returning a table with key
  * [l_1, ..., l_j, l_{j+1}, ..., l_n, r_{j+1}, ..., r_m].
  *
  * WARNING: If 'left' has any duplicate (full) key [k_1, ..., k_n], and j < m,
  * and 'right' has multiple rows with the corresponding join key
  * [k_1, ..., k_j] but distinct full keys, then the resulting table will have
  * out-of-order keys. To avoid this, ensure one of the following:
  * * j == m
  * * 'left' has distinct keys
  * * 'right' has distinct join keys (length j prefix), or at least no
  * distinct keys with the same join key.
  */
case class TableJoin(left: TableIR, right: TableIR, joinType: String, joinKey: Int)
  extends TableIR {

  require(joinKey >= 0)
  require(left.typ.key.length >= joinKey)
  require(right.typ.key.length >= joinKey)
  require(left.typ.keyType.truncate(joinKey) isIsomorphicTo right.typ.keyType.truncate(joinKey))
  require(left.typ.globalType.fieldNames.toSet
    .intersect(right.typ.globalType.fieldNames.toSet)
    .isEmpty)
  require(joinType == "inner" ||
    joinType == "left" ||
    joinType == "right" ||
    joinType == "outer" ||
    joinType == "zip")

  val children: IndexedSeq[BaseIR] = Array(left, right)

  lazy val rowCountUpperBound: Option[Long] = None

  private val newRowType = {
    val leftRowType = left.typ.rowType
    val rightRowType = right.typ.rowType
    val leftKey = left.typ.key.take(joinKey)
    val rightKey = right.typ.key.take(joinKey)

    val leftKeyType = TableType.keyType(leftRowType, leftKey)
    val leftValueType = TableType.valueType(leftRowType, leftKey)
    val rightValueType = TableType.valueType(rightRowType, rightKey)
    if (leftValueType.fieldNames.toSet
      .intersect(rightValueType.fieldNames.toSet)
      .nonEmpty)
      throw new RuntimeException(s"invalid join: \n  left value:  $leftValueType\n  right value: $rightValueType")

    leftKeyType ++ leftValueType ++ rightValueType
  }

  private val newGlobalType = left.typ.globalType ++ right.typ.globalType

  private val newKey = left.typ.key ++ right.typ.key.drop(joinKey)

  val typ: TableType = TableType(newRowType, newKey, newGlobalType)

  def copy(newChildren: IndexedSeq[BaseIR]): TableJoin = {
    assert(newChildren.length == 2)
    TableJoin(
      newChildren(0).asInstanceOf[TableIR],
      newChildren(1).asInstanceOf[TableIR],
      joinType,
      joinKey)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val leftTV = left.execute(ctx)
    val rightTV = right.execute(ctx)

    val newGlobals = BroadcastRow(ctx,
      Row.merge(leftTV.globals.javaValue, rightTV.globals.javaValue),
      newGlobalType)

    val leftRVDType = leftTV.rvd.typ.copy(key = left.typ.key.take(joinKey))
    val rightRVDType = rightTV.rvd.typ.copy(key = right.typ.key.take(joinKey))

    val leftRowType = leftRVDType.rowType
    val rightRowType = rightRVDType.rowType
    val leftKeyFieldIdx = leftRVDType.kFieldIdx
    val rightKeyFieldIdx = rightRVDType.kFieldIdx
    val leftValueFieldIdx = leftRVDType.valueFieldIdx
    val rightValueFieldIdx = rightRVDType.valueFieldIdx

    def noIndex(pfs: IndexedSeq[PField]): IndexedSeq[(String, PType)] =
      pfs.map(pf => (pf.name, pf.typ))

    def unionFieldPTypes(ps: PStruct, ps2: PStruct): IndexedSeq[(String, PType)] =
      ps.fields.zip(ps2.fields).map({
        case(pf1, pf2) => (pf1.name, InferPType.getNestedElementPTypes(Seq(pf1.typ, pf2.typ)))
      })

    def castFieldRequiredeness(ps: PStruct, required: Boolean): IndexedSeq[(String, PType)] =
      ps.fields.map(pf => (pf.name, pf.typ.setRequired(required)))

    val (lkT, lvT, rvT) = joinType match {
      case "inner" =>
        val keyTypeFields = castFieldRequiredeness(leftRVDType.kType, true)
        (keyTypeFields, noIndex(leftRVDType.valueType.fields), noIndex(rightRVDType.valueType.fields))
      case "left" =>
        val rValueTypeFields = castFieldRequiredeness(rightRVDType.valueType, false)
        (noIndex(leftRVDType.kType.fields), noIndex(leftRVDType.valueType.fields), rValueTypeFields)
      case "right" =>
        val keyTypeFields = leftRVDType.kType.fields.zip(rightRVDType.kType.fields).map({
          case(pf1, pf2) => {
            assert(pf1.typ isOfType pf2.typ)
            (pf1.name, pf2.typ)
          }
        })
        val lValueTypeFields = castFieldRequiredeness(leftRVDType.valueType, false)
        (keyTypeFields, lValueTypeFields, noIndex(rightRVDType.valueType.fields))
      case "outer" | "zip" =>
        val keyTypeFields = unionFieldPTypes(leftRVDType.kType, rightRVDType.kType)
        val lValueTypeFields = castFieldRequiredeness(leftRVDType.valueType, false)
        val rValueTypeFields = castFieldRequiredeness(rightRVDType.valueType, false)
        (keyTypeFields, lValueTypeFields, rValueTypeFields)
    }

    val newRowPType = PCanonicalStruct(true, lkT ++ lvT ++ rvT :_*)

    assert(newRowPType.virtualType == newRowType)

    val rvMerger = { (_: RVDContext, it: Iterator[JoinedRegionValue]) =>
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

        rvb.start(newRowPType)
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

    val joinedRVD = if (joinType == "zip") {
      val leftRVD = leftTV.rvd
      val rightRVD = rightTV.rvd
      leftRVD.orderedZipJoin(
        rightRVD,
        joinKey,
        rvMerger,
        RVDType(newRowPType, newKey),
        ctx)
    } else {
      val leftRVD = leftTV.rvd
      val rightRVD = rightTV.rvd
      leftRVD.orderedJoin(
        rightRVD,
        joinKey,
        joinType,
        rvMerger,
        RVDType(newRowPType, newKey),
        ctx)
    }

    TableValue(typ, newGlobals, joinedRVD)
  }
}

case class TableIntervalJoin(
  left: TableIR,
  right: TableIR,
  root: String,
  product: Boolean
) extends TableIR {
  lazy val children: IndexedSeq[BaseIR] = Array(left, right)

  lazy val rowCountUpperBound: Option[Long] = left.rowCountUpperBound

  val rightType: Type = if (product) TArray(right.typ.valueType) else right.typ.valueType
  val typ: TableType = left.typ.copy(rowType = left.typ.rowType.appendKey(root, rightType))

  override def copy(newChildren: IndexedSeq[BaseIR]): TableIR =
    TableIntervalJoin(newChildren(0).asInstanceOf[TableIR], newChildren(1).asInstanceOf[TableIR], root, product)

  override def partitionCounts: Option[IndexedSeq[Long]] = left.partitionCounts

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val leftValue = left.execute(ctx)
    val rightValue = right.execute(ctx)

    val leftRVDType = leftValue.rvd.typ
    val rightRVDType = rightValue.rvd.typ
    val rightValueFields = rightRVDType.valueType.fieldNames

    val localKey = typ.key
    val localRoot = root
    val newRVD =
      if (product) {
        val joiner = (rightPType: PStruct) => {
          val leftRowType = leftRVDType.rowType
          val newRowType = leftRowType.appendKey(localRoot, PCanonicalArray(rightPType.selectFields(rightValueFields)))
          (RVDType(newRowType, localKey), (_: RVDContext, it: Iterator[Muple[RegionValue, Iterable[RegionValue]]]) => {
            val rvb = new RegionValueBuilder()
            val rv2 = RegionValue()
            it.map { case Muple(rv, is) =>
              rvb.set(rv.region)
              rvb.start(newRowType)
              rvb.startStruct()
              rvb.addAllFields(leftRowType, rv)
              rvb.startArray(is.size)
              is.foreach(i => rvb.selectRegionValue(rightPType, rightRVDType.valueFieldIdx, i))
              rvb.endArray()
              rvb.endStruct()
              rv2.set(rv.region, rvb.end())

              rv2
            }
          })
        }

        leftValue.rvd.orderedLeftIntervalJoin(rightValue.rvd, joiner)
      } else {
        val joiner = (rightPType: PStruct) => {
          val leftRowType = leftRVDType.rowType
          val newRowType = leftRowType.appendKey(localRoot, rightPType.selectFields(rightValueFields).setRequired(false))

          (RVDType(newRowType, localKey), (_: RVDContext, it: Iterator[JoinedRegionValue]) => {
            val rvb = new RegionValueBuilder()
            val rv2 = RegionValue()
            it.map { case Muple(rv, i) =>
              rvb.set(rv.region)
              rvb.start(newRowType)
              rvb.startStruct()
              rvb.addAllFields(leftRowType, rv)
              if (i == null)
                rvb.setMissing()
              else
                rvb.selectRegionValue(rightPType, rightRVDType.valueFieldIdx, i)
              rvb.endStruct()
              rv2.set(rv.region, rvb.end())

              rv2
            }
          })
        }

        leftValue.rvd.orderedLeftIntervalJoinDistinct(rightValue.rvd, joiner)
      }

    TableValue(typ, leftValue.globals, newRVD)
  }
}

case class TableZipUnchecked(left: TableIR, right: TableIR) extends TableIR {
  require((left.typ.rowType.fieldNames ++ right.typ.rowType.fieldNames).areDistinct())
  require(right.typ.key.isEmpty)

  lazy val rowCountUpperBound: Option[Long] = (left.rowCountUpperBound, right.rowCountUpperBound) match {
    case (Some(l), Some(r)) => Some(l.min(r))
    case (Some(l), None) => Some(l)
    case (None, Some(r)) => Some(r)
    case (None, None) => None
  }

  val typ: TableType = left.typ.copy(rowType = left.typ.rowType ++ right.typ.rowType)

  override def partitionCounts: Option[IndexedSeq[Long]] = left.partitionCounts

  lazy val children: IndexedSeq[BaseIR] = Array(left, right)

  def copy(newChildren: IndexedSeq[BaseIR]): TableIR = {
    val IndexedSeq(newLeft: TableIR, newRight: TableIR) = newChildren
    TableZipUnchecked(newLeft, newRight)
  }

  override def execute(ctx: ExecuteContext): TableValue = {
    val tv1 = left.execute(ctx)
    val tv2 = right.execute(ctx)

    val inserter = InsertFields(
      Ref("left", left.typ.rowType),
      right.typ.rowType.fieldNames.map(f => f -> GetField(Ref("right", right.typ.rowType), f)))

    val rvdType: RVDType = RVDType(inserter.pType, tv1.rvd.typ.key)

    val (t2, makeF) = ir.Compile[AsmFunction3RegionLongLongLong](
      ctx,
      FastIndexedSeq(("left", tv1.rvd.typ.rowType),
        ("right", tv2.rvd.typ.rowType)),
      FastIndexedSeq(classInfo[Region], LongInfo, LongInfo), LongInfo,
      inserter)

    assert(t2.virtualType == typ.rowType)
    assert(t2 == rvdType.rowType)

    val rvd = tv1.rvd.zipPartitionsWithIndex(rvdType, tv2.rvd) { (i, ctx, it1, it2) =>
      val partRegion = ctx.partitionRegion
      val f = makeF(i, partRegion)
      val region = ctx.region
      val rv3 = RegionValue(region)
      new Iterator[RegionValue] {
        def hasNext: Boolean = {
          val hn1 = it1.hasNext
          val hn2 = it2.hasNext
          assert(hn1 == hn2)
          hn1
        }

        def next(): RegionValue = {
          val rv1 = it1.next()
          val rv2 = it2.next()
          val off = f(region, rv1.offset, rv2.offset)
          rv3.set(region, off)
          rv3
        }
      }
    }
    TableValue(typ, tv1.globals, rvd)
  }
}

case class TableMultiWayZipJoin(children: IndexedSeq[TableIR], fieldName: String, globalName: String) extends TableIR {
  require(children.length > 0, "there must be at least one table as an argument")

  private val first = children.head
  private val rest = children.tail

  lazy val rowCountUpperBound: Option[Long] = None

  require(rest.forall(e => e.typ.rowType == first.typ.rowType), "all rows must have the same type")
  require(rest.forall(e => e.typ.key == first.typ.key), "all keys must be the same")
  require(rest.forall(e => e.typ.globalType == first.typ.globalType),
    "all globals must have the same type")

  private val newGlobalType = TStruct(globalName -> TArray(first.typ.globalType))
  private val newValueType = TStruct(fieldName -> TArray(first.typ.valueType))
  private val newRowType = first.typ.keyType ++ newValueType

  lazy val typ: TableType = first.typ.copy(
    rowType = newRowType,
    globalType = newGlobalType
  )

  def copy(newChildren: IndexedSeq[BaseIR]): TableMultiWayZipJoin =
    TableMultiWayZipJoin(newChildren.asInstanceOf[IndexedSeq[TableIR]], fieldName, globalName)

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val childValues = children.map(_.execute(ctx))

    val childRVDs = RVD.unify(childValues.map(_.rvd)).toFastIndexedSeq
    assert(childRVDs.forall(_.typ.key.startsWith(typ.key)))

    val repartitionedRVDs =
      if (childRVDs(0).partitioner.satisfiesAllowedOverlap(typ.key.length - 1) &&
        childRVDs.forall(rvd => rvd.partitioner == childRVDs(0).partitioner))
        childRVDs.map(_.truncateKey(typ.key.length))
      else {
        info("TableMultiWayZipJoin: repartitioning children")
        val childRanges = childRVDs.flatMap(_.partitioner.coarsenedRangeBounds(typ.key.length))
        val newPartitioner = RVDPartitioner.generate(typ.keyType, childRanges)
        childRVDs.map(_.repartition(newPartitioner, ctx))
      }
    val newPartitioner = repartitionedRVDs(0).partitioner

    val rvdType = repartitionedRVDs(0).typ
    val rowType = rvdType.rowType
    val keyIdx = rvdType.kFieldIdx
    val valIdx = rvdType.valueFieldIdx
    val localRVDType = rvdType
    val keyFields = rvdType.kType.fields.map(f => (f.name, f.typ))
    val valueFields = rvdType.valueType.fields.map(f => (f.name, f.typ))
    val localNewRowType = PCanonicalStruct(required = true,
      keyFields ++ Array((fieldName, PCanonicalArray(
        PCanonicalStruct(required = false, valueFields: _*)))): _*)
    val localDataLength = children.length
    val rvMerger = { (ctx: RVDContext, it: Iterator[ArrayBuilder[(RegionValue, Int)]]) =>
      val rvb = new RegionValueBuilder()
      val newRegionValue = RegionValue()

      it.map { rvs =>
        val rv = rvs(0)._1
        rvb.set(ctx.region)
        rvb.start(localNewRowType)
        rvb.startStruct()
        rvb.addFields(rowType, rv, keyIdx) // Add the key
        rvb.startMissingArray(localDataLength) // add the values
      var i = 0
        while (i < rvs.length) {
          val (rv, j) = rvs(i)
          rvb.setArrayIndex(j)
          rvb.setPresent()
          rvb.startStruct()
          rvb.addFields(rowType, rv, valIdx)
          rvb.endStruct()
          i += 1
        }
        rvb.endArrayUnchecked()
        rvb.endStruct()

        newRegionValue.set(rvb.region, rvb.end())
        newRegionValue
      }
    }

    val rvd = RVD(
      typ = RVDType(localNewRowType, typ.key),
      partitioner = newPartitioner,
      crdd = ContextRDD.czipNPartitions(repartitionedRVDs.map(_.crdd.toCRDDRegionValue)) { (ctx, its) =>
        val orvIters = its.map(it => OrderedRVIterator(localRVDType, it, ctx))
        rvMerger(ctx, OrderedRVIterator.multiZipJoin(orvIters))
      }.toCRDDPtr)

    val newGlobals = BroadcastRow(ctx,
      Row(childValues.map(_.globals.javaValue)),
      newGlobalType)

    TableValue(typ, newGlobals, rvd)
  }
}

case class TableLeftJoinRightDistinct(left: TableIR, right: TableIR, root: String) extends TableIR {
  require(right.typ.keyType isPrefixOf left.typ.keyType,
    s"\n  L: ${ left.typ }\n  R: ${ right.typ }")

  lazy val rowCountUpperBound: Option[Long] = left.rowCountUpperBound

  lazy val children: IndexedSeq[BaseIR] = Array(left, right)

  private val newRowType = left.typ.rowType.structInsert(right.typ.valueType, List(root))._1
  val typ: TableType = left.typ.copy(rowType = newRowType)

  override def partitionCounts: Option[IndexedSeq[Long]] = left.partitionCounts

  def copy(newChildren: IndexedSeq[BaseIR]): TableLeftJoinRightDistinct = {
    val IndexedSeq(newLeft: TableIR, newRight: TableIR) = newChildren
    TableLeftJoinRightDistinct(newLeft, newRight, root)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val leftValue = left.execute(ctx)
    val rightValue = right.execute(ctx)

    val joinKey = math.min(left.typ.key.length, right.typ.key.length)
    leftValue.copy(
      typ = typ,
      rvd = leftValue.rvd
        .orderedLeftJoinDistinctAndInsert(rightValue.rvd.truncateKey(joinKey), root))
  }
}

// Must leave key fields unchanged.
case class TableMapRows(child: TableIR, newRow: IR) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array(child, newRow)

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  val typ: TableType = child.typ.copy(rowType = newRow.typ.asInstanceOf[TStruct])

  def copy(newChildren: IndexedSeq[BaseIR]): TableMapRows = {
    assert(newChildren.length == 2)
    TableMapRows(newChildren(0).asInstanceOf[TableIR], newChildren(1).asInstanceOf[IR])
  }

  override def partitionCounts: Option[IndexedSeq[Long]] = child.partitionCounts

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val tv = child.execute(ctx)

    val scanRef = genUID()
    val extracted = agg.Extract.apply(agg.Extract.liftScan(newRow), scanRef)

    if (extracted.aggs.isEmpty) {
      val (rTyp, f) = ir.Compile[AsmFunction3RegionLongLongLong](
        ctx,
        FastIndexedSeq(("global", tv.globals.t),
          ("row", tv.rvd.rowPType)),
        FastIndexedSeq(classInfo[Region], LongInfo, LongInfo), LongInfo,
        Coalesce(FastIndexedSeq(
          extracted.postAggIR,
            Die("Internal error: TableMapRows: row expression missing", extracted.postAggIR.typ))))

      val rowIterationNeedsGlobals = Mentions(extracted.postAggIR, "global")
      val globalsBc =
        if (rowIterationNeedsGlobals)
          tv.globals.broadcast
        else
          null

      val itF = { (i: Int, ctx: RVDContext, it: Iterator[Long]) =>
        val globalRegion = ctx.partitionRegion
        val globals = if (rowIterationNeedsGlobals)
          globalsBc.value.readRegionValue(globalRegion)
        else
          0

        val newRow = f(i, globalRegion)
        it.map { ptr =>
          newRow(ctx.r, globals, ptr)
        }
      }

      return tv.copy(
        typ = typ,
        rvd = tv.rvd.mapPartitionsWithIndex(RVDType(rTyp.asInstanceOf[PStruct], typ.key))(itF))
    }

    val physicalAggs = extracted.getPhysicalAggs(
      ctx,
      Env("global" -> tv.globals.t),
      Env("global" -> tv.globals.t, "row" -> tv.rvd.rowPType)
    )

    val scanInitNeedsGlobals = Mentions(extracted.init, "global")
    val scanSeqNeedsGlobals = Mentions(extracted.seqPerElt, "global")
    val rowIterationNeedsGlobals = Mentions(extracted.postAggIR, "global")

    val globalsBc =
      if (rowIterationNeedsGlobals || scanInitNeedsGlobals || scanSeqNeedsGlobals)
        tv.globals.broadcast
      else
        null

    val spec = BufferSpec.defaultUncompressed

    // Order of operations:
    // 1. init op on all aggs and serialize to byte array.
    // 2. load in init op on each partition, seq op over partition, serialize.
    // 3. load in partition aggregations, comb op as necessary, serialize.
    // 4. load in partStarts, calculate newRow based on those results.

    val (_, initF) = ir.CompileWithAggregators2[AsmFunction2RegionLongUnit](ctx,
      physicalAggs,
      FastIndexedSeq(("global", tv.globals.t)),
      FastIndexedSeq(classInfo[Region], LongInfo), UnitInfo,
      Begin(FastIndexedSeq(extracted.init, extracted.serializeSet(0, 0, spec))))

    val (_, eltSeqF) = ir.CompileWithAggregators2[AsmFunction3RegionLongLongUnit](ctx,
      physicalAggs,
      FastIndexedSeq(("global", tv.globals.t),
        ("row", tv.rvd.rowPType)),
      FastIndexedSeq(classInfo[Region], LongInfo, LongInfo), UnitInfo,
      extracted.eltOp(ctx))

    val read = extracted.deserialize(ctx, spec, physicalAggs)
    val write = extracted.serialize(ctx, spec, physicalAggs)
    val combOpF = extracted.combOpF(ctx, spec, physicalAggs)

    val (rTyp, f) = ir.CompileWithAggregators2[AsmFunction3RegionLongLongLong](ctx,
      physicalAggs,
      FastIndexedSeq(("global", tv.globals.t),
        ("row", tv.rvd.rowPType)),
      FastIndexedSeq(classInfo[Region], LongInfo, LongInfo), UnitInfo,
      Let(scanRef, extracted.results,
        Coalesce(FastIndexedSeq(
          extracted.postAggIR,
          Die("Internal error: TableMapRows: row expression missing", extracted.postAggIR.typ)))))
    assert(rTyp.virtualType == newRow.typ)

    // 1. init op on all aggs and write out to initPath
    val initAgg = Region.scoped { aggRegion =>
      Region.scoped { fRegion =>
        val init = initF(0, fRegion)
        init.newAggState(aggRegion)
        init(fRegion, tv.globals.value.offset)
        init.getSerializedAgg(0)
      }
    }

    val hc = HailContext.get
    if (hc.flags.get("distributed_scan_comb_op") != null) {
      val fs = hc.fsBc
      val tmpBase = hc.tmpDir
      val d = digitsNeeded(tv.rvd.getNumPartitions)
      val files = tv.rvd.mapPartitionsWithIndex { (i, ctx, it) =>
        val path = tmpBase + "/" + partFile(d, i, TaskContext.get)
        val globalRegion = ctx.freshRegion
        val globals = if (scanSeqNeedsGlobals) globalsBc.value.readRegionValue(globalRegion) else 0

        Region.smallScoped { aggRegion =>
          val seq = eltSeqF(i, globalRegion)

          seq.setAggState(aggRegion, read(aggRegion, initAgg))
          it.foreach { ptr =>
            seq(ctx.region, globals, ptr)
            ctx.region.clear()
          }
          using(new DataOutputStream(fs.value.create(path))) { os =>
            val bytes = write(aggRegion, seq.getAggOffset())
            os.writeInt(bytes.length)
            os.write(bytes)
          }
          Iterator.single(path)
        }
      }.collect()

      val fileStack = new ArrayBuilder[Array[String]]()
      var filesToMerge: Array[String] = files
      while (filesToMerge.length > 1) {
        val nToMerge = filesToMerge.length / 2
        log.info(s"Running combOp stage with $nToMerge tasks")
        fileStack += filesToMerge
        filesToMerge = hc.sc.parallelize(0 until nToMerge, nToMerge)
          .mapPartitions { it =>
            val i = it.next()
            assert(it.isEmpty)
            val path = tmpBase + "/" + partFile(d, i, TaskContext.get)
            val file1 = filesToMerge(i * 2)
            val file2 = filesToMerge(i * 2 + 1)
            def readToBytes(is: DataInputStream): Array[Byte] = {
              val len = is.readInt()
              val b = new Array[Byte](len)
              is.readFully(b)
              b
            }
            val b1 = using(new DataInputStream(fs.value.open(file1)))(readToBytes)
            val b2 = using(new DataInputStream(fs.value.open(file2)))(readToBytes)
            using(new DataOutputStream(fs.value.create(path))) { os =>
              val bytes = combOpF(b1, b2)
              os.writeInt(bytes.length)
              os.write(bytes)
            }
            Iterator.single(path)
          }.collect()
      }
      fileStack += filesToMerge

      val itF = { (i: Int, ctx: RVDContext, it: Iterator[Long]) =>
        val globalRegion = ctx.freshRegion
        val globals = if (rowIterationNeedsGlobals || scanSeqNeedsGlobals)
          globalsBc.value.readRegionValue(globalRegion)
        else
          0
        val partitionAggs = {
          var j = 0
          var x = i
          val ab = new ArrayBuilder[String]
          while (j < fileStack.length) {
            assert(x <= fileStack(j).length)
            if (x % 2 != 0) {
              x -= 1
              ab += fileStack(j)(x)
            }
            assert(x % 2 == 0)
            x = x / 2
            j += 1
          }
          assert(x == 0)
          var b = initAgg
          ab.result().reverseIterator.foreach { path =>
            def readToBytes(is: DataInputStream): Array[Byte] = {
              val len = is.readInt()
              val b = new Array[Byte](len)
              is.readFully(b)
              b
            }

            b = combOpF(b, using(new DataInputStream(fs.value.open(path)))(readToBytes))
          }
          b
        }

        val aggRegion = ctx.freshRegion
        val newRow = f(i, globalRegion)
        val seq = eltSeqF(i, globalRegion)
        var aggOff = read(aggRegion, partitionAggs)

        it.map { ptr =>
          newRow.setAggState(aggRegion, aggOff)
          val newPtr = newRow(ctx.region, globals, ptr)
          seq.setAggState(aggRegion, newRow.getAggOffset())
          seq(ctx.region, globals, ptr)
          aggOff = seq.getAggOffset()
          newPtr
        }
      }
      return tv.copy(
        typ = typ,
        rvd = tv.rvd.mapPartitionsWithIndex(RVDType(rTyp.asInstanceOf[PStruct], typ.key))(itF))
    }

    // 2. load in init op on each partition, seq op over partition, write out.
    val scanPartitionAggs = SpillingCollectIterator(tv.rvd.mapPartitionsWithIndex { (i, ctx, it) =>
      val globalRegion = ctx.partitionRegion
      val globals = if (scanSeqNeedsGlobals) globalsBc.value.readRegionValue(globalRegion) else 0

      Region.smallScoped { aggRegion =>
        val seq = eltSeqF(i, globalRegion)

        seq.setAggState(aggRegion, read(aggRegion, initAgg))
        it.foreach { ptr =>
          seq(ctx.region, globals, ptr)
          ctx.region.clear()
        }
        Iterator.single(write(aggRegion, seq.getAggOffset()))
      }
    }, HailContext.get.flags.get("max_leader_scans").toInt)


    // 3. load in partition aggregations, comb op as necessary, write back out.
    val partAggs = scanPartitionAggs.scanLeft(initAgg)(combOpF)
    val scanAggCount = tv.rvd.getNumPartitions
    val partitionIndices = new Array[Long](scanAggCount)
    val scanAggsPerPartitionFile = HailContext.get.getTemporaryFile()
    using(HailContext.get.fs.createNoCompression(scanAggsPerPartitionFile)) { os =>
      partAggs.zipWithIndex.foreach { case (x, i) =>
        if (i < scanAggCount) {
          partitionIndices(i) = os.getPosition
          os.writeInt(x.length)
          os.write(x, 0, x.length)
        }
      }
    }

    val bcFS = HailContext.get.fsBc

    // 4. load in partStarts, calculate newRow based on those results.
    val itF = { (i: Int, ctx: RVDContext, filePosition: Long, it: Iterator[Long]) =>
      val globalRegion = ctx.partitionRegion
      val globals = if (rowIterationNeedsGlobals || scanSeqNeedsGlobals)
        globalsBc.value.readRegionValue(globalRegion)
      else
        0
      val partitionAggs = using(bcFS.value.openNoCompression(scanAggsPerPartitionFile)) { is =>
        is.seek(filePosition)
        val aggSize = is.readInt()
        val partAggs = new Array[Byte](aggSize)
        var nread = is.read(partAggs, 0, aggSize)
        var r = nread
        while (r > 0 && nread < aggSize) {
          r = is.read(partAggs, nread, aggSize - nread)
          if (r > 0) nread += r
        }
        if (nread != aggSize) {
          fatal(s"aggs read wrong number of bytes: $nread vs $aggSize")
        }
        partAggs
      }

      val aggRegion = ctx.freshRegion
      val newRow = f(i, globalRegion)
      val seq = eltSeqF(i, globalRegion)
      var aggOff = read(aggRegion, partitionAggs)

      it.map { ptr =>
        newRow.setAggState(aggRegion, aggOff)
        val off = newRow(ctx.region, globals, ptr)
        seq.setAggState(aggRegion, newRow.getAggOffset())
        seq(ctx.region, globals, ptr)
        aggOff = seq.getAggOffset()
        off
      }
    }
    tv.copy(
      typ = typ,
      rvd = tv.rvd.mapPartitionsWithIndexAndValue(RVDType(rTyp.asInstanceOf[PStruct], typ.key), partitionIndices)(itF))
   }
}

case class TableMapGlobals(child: TableIR, newGlobals: IR) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array(child, newGlobals)

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  val typ: TableType =
    child.typ.copy(globalType = newGlobals.typ.asInstanceOf[TStruct])

  def copy(newChildren: IndexedSeq[BaseIR]): TableMapGlobals = {
    assert(newChildren.length == 2)
    TableMapGlobals(newChildren(0).asInstanceOf[TableIR], newChildren(1).asInstanceOf[IR])
  }

  override def partitionCounts: Option[IndexedSeq[Long]] = child.partitionCounts

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val tv = child.execute(ctx)

    val (resultPType, f) = Compile[AsmFunction2RegionLongLong](ctx,
      FastIndexedSeq(("global", tv.globals.t)),
      FastIndexedSeq(classInfo[Region], LongInfo), LongInfo,
      Coalesce(FastIndexedSeq(
        newGlobals,
        Die("Internal error: TableMapGlobals: globals missing", newGlobals.typ))))

    val resultOff = f(0, ctx.r)(ctx.r, tv.globals.value.offset)
    tv.copy(typ = typ,
      globals = BroadcastRow(RegionValue(ctx.r, resultOff), resultPType.asInstanceOf[PStruct], HailContext.get.backend))
  }
}

case class TableExplode(child: TableIR, path: IndexedSeq[String]) extends TableIR {
  assert(path.nonEmpty)
  assert(!child.typ.key.contains(path.head))

  lazy val rowCountUpperBound: Option[Long] = None

  lazy val children: IndexedSeq[BaseIR] = Array(child)

  private val childRowType = child.typ.rowType

  private val length: IR = {
    Coalesce(FastIndexedSeq(
      ArrayLen(CastToArray(
        path.foldLeft[IR](Ref("row", childRowType))((struct, field) =>
          GetField(struct, field)))),
      0))
  }

  val idx = Ref(genUID(), TInt32)
  val newRow: InsertFields = {
    val refs = path.init.scanLeft(Ref("row", childRowType))((struct, name) =>
      Ref(genUID(), coerce[TStruct](struct.typ).field(name).typ))

    path.zip(refs).zipWithIndex.foldRight[IR](idx) {
      case (((field, ref), i), arg) =>
        InsertFields(ref, FastIndexedSeq(field ->
          (if (i == refs.length - 1)
            ArrayRef(CastToArray(GetField(ref, field)), arg)
          else
            Let(refs(i + 1).name, GetField(ref, field), arg))))
    }.asInstanceOf[InsertFields]
  }

  val typ: TableType = child.typ.copy(rowType = newRow.typ)

  def copy(newChildren: IndexedSeq[BaseIR]): TableExplode = {
    assert(newChildren.length == 1)
    TableExplode(newChildren(0).asInstanceOf[TableIR], path)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val prev = child.execute(ctx)

    val (len, l) = Compile[AsmFunction2RegionLongInt](ctx,
      FastIndexedSeq(("row", prev.rvd.rowPType)),
      FastIndexedSeq(classInfo[Region], LongInfo), IntInfo,
      length)
    val (newRowType: PStruct, f) = Compile[AsmFunction3RegionLongIntLong](
      ctx,
      FastIndexedSeq(("row", prev.rvd.rowPType),
        (idx.name, PInt32(true))),
      FastIndexedSeq(classInfo[Region], LongInfo, IntInfo), LongInfo,
      newRow)
    assert(newRowType.virtualType == typ.rowType)

    val rvdType: RVDType = RVDType(
      newRowType,
      prev.rvd.typ.key.takeWhile(_ != path.head)
    )
    TableValue(typ,
      prev.globals,
      prev.rvd.boundary.mapPartitionsWithIndex(rvdType) { (i, ctx, it) =>
        val globalRegion = ctx.partitionRegion
        val lenF = l(i, globalRegion)
        val rowF = f(i, globalRegion)
        it.flatMap { ptr =>
          val len = lenF(ctx.region, ptr)
          new Iterator[Long] {
            private[this] var i = 0

            def hasNext: Boolean = i < len

            def next(): Long = {
              val ret = rowF(ctx.region, ptr, i)
              i += 1
              ret
            }
          }
        }
      })
  }
}

case class TableUnion(children: IndexedSeq[TableIR]) extends TableIR {
  assert(children.nonEmpty)
  assert(children.tail.forall(_.typ.rowType == children(0).typ.rowType))
  assert(children.tail.forall(_.typ.key == children(0).typ.key))

  lazy val rowCountUpperBound: Option[Long] = {
    val definedChildren = children.flatMap(_.rowCountUpperBound)
    if (definedChildren.length == children.length)
      Some(definedChildren.sum)
    else
      None
  }

  def copy(newChildren: IndexedSeq[BaseIR]): TableUnion = {
    TableUnion(newChildren.map(_.asInstanceOf[TableIR]))
  }

  val typ: TableType = children(0).typ

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val tvs = children.map(_.execute(ctx))
    tvs(0).copy(
      rvd = RVD.union(RVD.unify(tvs.map(_.rvd)), tvs(0).typ.key.length, ctx))
  }
}

case class MatrixRowsTable(child: MatrixIR) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array(child)

  override def partitionCounts: Option[IndexedSeq[Long]] = child.partitionCounts

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  def copy(newChildren: IndexedSeq[BaseIR]): MatrixRowsTable = {
    assert(newChildren.length == 1)
    MatrixRowsTable(newChildren(0).asInstanceOf[MatrixIR])
  }

  val typ: TableType = child.typ.rowsTableType
}

case class MatrixColsTable(child: MatrixIR) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array(child)

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  def copy(newChildren: IndexedSeq[BaseIR]): MatrixColsTable = {
    assert(newChildren.length == 1)
    MatrixColsTable(newChildren(0).asInstanceOf[MatrixIR])
  }

  val typ: TableType = child.typ.colsTableType
}

case class MatrixEntriesTable(child: MatrixIR) extends TableIR {
  val children: IndexedSeq[BaseIR] = Array(child)

  lazy val rowCountUpperBound: Option[Long] = None

  def copy(newChildren: IndexedSeq[BaseIR]): MatrixEntriesTable = {
    assert(newChildren.length == 1)
    MatrixEntriesTable(newChildren(0).asInstanceOf[MatrixIR])
  }

  val typ: TableType = child.typ.entriesTableType
}

case class TableDistinct(child: TableIR) extends TableIR {
  lazy val children: IndexedSeq[BaseIR] = Array(child)

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  def copy(newChildren: IndexedSeq[BaseIR]): TableDistinct = {
    val IndexedSeq(newChild) = newChildren
    TableDistinct(newChild.asInstanceOf[TableIR])
  }

  val typ: TableType = child.typ

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val prev = child.execute(ctx)
    prev.copy(rvd = prev.rvd.truncateKey(prev.typ.key).distinctByKey(ctx))
  }
}

case class TableKeyByAndAggregate(
  child: TableIR,
  expr: IR,
  newKey: IR,
  nPartitions: Option[Int] = None,
  bufferSize: Int = 50) extends TableIR {
  require(expr.typ.isInstanceOf[TStruct])
  require(newKey.typ.isInstanceOf[TStruct])
  require(bufferSize > 0)

  lazy val children: IndexedSeq[BaseIR] = Array(child, expr, newKey)

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  def copy(newChildren: IndexedSeq[BaseIR]): TableKeyByAndAggregate = {
    val IndexedSeq(newChild: TableIR, newExpr: IR, newNewKey: IR) = newChildren
    TableKeyByAndAggregate(newChild, newExpr, newNewKey, nPartitions, bufferSize)
  }

  private val keyType = newKey.typ.asInstanceOf[TStruct]
  val typ: TableType = TableType(rowType = keyType ++ coerce[TStruct](expr.typ),
    globalType = child.typ.globalType,
    key = keyType.fieldNames
  )

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val prev = child.execute(ctx)

    val localKeyType = keyType
    val (localKeyPType: PStruct, makeKeyF) = ir.Compile[AsmFunction3RegionLongLongLong](ctx,
      FastIndexedSeq(("row", prev.rvd.rowPType),
        ("global", prev.globals.t)),
      FastIndexedSeq(classInfo[Region], LongInfo, LongInfo), LongInfo,
      Coalesce(FastIndexedSeq(
        newKey,
        Die("Internal error: TableKeyByAndAggregate: newKey missing", newKey.typ))))

    val globalsBc = prev.globals.broadcast

    val spec = BufferSpec.defaultUncompressed
    val res = genUID()
    val extracted = agg.Extract(expr, res)

    val physicalAggs = extracted.getPhysicalAggs(
      ctx,
      Env("global" -> prev.globals.t),
      Env("global" -> prev.globals.t, "row" -> prev.rvd.rowPType)
    )

    val (_, makeInit) = ir.CompileWithAggregators2[AsmFunction2RegionLongUnit](ctx,
      physicalAggs,
      FastIndexedSeq(("global", prev.globals.t)),
      FastIndexedSeq(classInfo[Region], LongInfo), UnitInfo,
      extracted.init)

    val (_, makeSeq) = ir.CompileWithAggregators2[AsmFunction3RegionLongLongUnit](ctx,
      physicalAggs,
      FastIndexedSeq(("global", prev.globals.t),
        ("row", prev.rvd.rowPType)),
      FastIndexedSeq(classInfo[Region], LongInfo, LongInfo), UnitInfo,
      extracted.seqPerElt)

    val (rTyp: PStruct, makeAnnotate) = ir.CompileWithAggregators2[AsmFunction2RegionLongLong](ctx,
      physicalAggs,
      FastIndexedSeq(("global", prev.globals.t)),
      FastIndexedSeq(classInfo[Region], LongInfo), LongInfo,
      Let(res, extracted.results, extracted.postAggIR))
    assert(rTyp.virtualType == typ.valueType, s"$rTyp, ${ typ.valueType }")

    val serialize = extracted.serialize(ctx, spec, physicalAggs)
    val deserialize = extracted.deserialize(ctx, spec, physicalAggs)
    val combOp = extracted.combOpF(ctx, spec, physicalAggs)

    val initF = makeInit(0, ctx.r)
    val globalsOffset = prev.globals.value.offset
    val initAggs = Region.scoped { aggRegion =>
      initF.newAggState(aggRegion)
      initF(ctx.r, globalsOffset)
      serialize(aggRegion, initF.getAggOffset())
    }

    val newRowType = PCanonicalStruct(required = true,
      localKeyPType.fields.map(f => (f.name, PType.canonical(f.typ))) ++ rTyp.fields.map(f => (f.name, f.typ)): _*)

    val localBufferSize = bufferSize
    val rdd = prev.rvd
      .boundary
      .mapPartitionsWithIndex { (i, ctx, it) =>
        val partRegion = ctx.partitionRegion
        val globals = globalsBc.value.readRegionValue(partRegion)
        val makeKey = {
          val f = makeKeyF(i, partRegion)
          ptr: Long => {
            val keyOff = f(ctx.region, ptr, globals)
            SafeRow.read(localKeyPType, keyOff).asInstanceOf[Row]
          }
        }
        val makeAgg = { () =>
          val aggRegion = ctx.freshRegion
          RegionValue(aggRegion, deserialize(aggRegion, initAggs))
        }

        val seqOp = {
          val f = makeSeq(i, partRegion)
          (ptr: Long, agg: RegionValue) => {
            f.setAggState(agg.region, agg.offset)
            f(ctx.region, globals, ptr)
            agg.setOffset(f.getAggOffset())
          }
        }
        val serializeAndCleanupAggs = { rv: RegionValue =>
          val a = serialize(rv.region, rv.offset)
          rv.region.close()
          a
        }

        new BufferedAggregatorIterator[Long, RegionValue, Array[Byte], Row](
          it,
          makeAgg,
          makeKey,
          seqOp,
          serializeAndCleanupAggs,
          localBufferSize)
      }.aggregateByKey(initAggs, nPartitions.getOrElse(prev.rvd.getNumPartitions))(combOp, combOp)

    val crdd = ContextRDD.weaken(rdd).cmapPartitionsWithIndex(
      { (i, ctx, it) =>
        val region = ctx.region

        val rvb = new RegionValueBuilder()
        val partRegion = ctx.partitionRegion
        val globals = globalsBc.value.readRegionValue(partRegion)
        val annotate = makeAnnotate(i, partRegion)

        it.map { case (key, aggs) =>
          rvb.set(region)
          rvb.start(newRowType)
          rvb.startStruct()
          var i = 0
          while (i < localKeyType.size) {
            rvb.addAnnotation(localKeyType.types(i), key.get(i))
            i += 1
          }

          val aggOff = deserialize(region, aggs)
          annotate.setAggState(region, aggOff)
          rvb.addAllFields(rTyp, region, annotate(region, globals))
          rvb.endStruct()
          rvb.end()
        }
      })

    prev.copy(
      typ = typ,
      rvd = RVD.coerce(RVDType(newRowType, keyType.fieldNames), crdd, ctx))
  }
}

// follows key_by non-empty key
case class TableAggregateByKey(child: TableIR, expr: IR) extends TableIR {
  require(child.typ.key.nonEmpty)

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  lazy val children: IndexedSeq[BaseIR] = Array(child, expr)

  def copy(newChildren: IndexedSeq[BaseIR]): TableAggregateByKey = {
    assert(newChildren.length == 2)
    val IndexedSeq(newChild: TableIR, newExpr: IR) = newChildren
    TableAggregateByKey(newChild, newExpr)
  }

  val typ: TableType = child.typ.copy(rowType = child.typ.keyType ++ coerce[TStruct](expr.typ))

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val prev = child.execute(ctx)
    val prevRVD = prev.rvd

    val res = genUID()
    val extracted = agg.Extract(expr, res)

    val physicalAggs = extracted.getPhysicalAggs(
      ctx,
      Env("global" -> prev.globals.t),
      Env("global" -> prev.globals.t, "row" -> prev.rvd.rowPType)
    )

    val (_, makeInit) = ir.CompileWithAggregators2[AsmFunction2RegionLongUnit](ctx,
      physicalAggs,
      FastIndexedSeq(("global", prev.globals.t)),
      FastIndexedSeq(classInfo[Region], LongInfo), UnitInfo,
      extracted.init)

    val (_, makeSeq) = ir.CompileWithAggregators2[AsmFunction3RegionLongLongUnit](ctx,
      physicalAggs,
      FastIndexedSeq(("global", prev.globals.t),
        ("row", prev.rvd.rowPType)),
      FastIndexedSeq(classInfo[Region], LongInfo, LongInfo), UnitInfo,
      extracted.seqPerElt)

    val valueIR = Let(res, extracted.results, extracted.postAggIR)
    val keyType = prev.rvd.typ.kType
    assert(keyType.virtualType == prev.typ.keyType)

    val key = Ref(genUID(), keyType.virtualType)
    val value = Ref(genUID(), valueIR.typ)
    val (rowType: PStruct, makeRow) = ir.CompileWithAggregators2[AsmFunction3RegionLongLongLong](ctx,
      physicalAggs,
      FastIndexedSeq(("global", prev.globals.t),
        (key.name, keyType)),
      FastIndexedSeq(classInfo[Region], LongInfo, LongInfo), LongInfo,
      Let(value.name, valueIR,
        InsertFields(key, typ.valueType.fieldNames.map(n => n -> GetField(value, n)))))

    assert(rowType.virtualType == typ.rowType, s"$rowType, ${ typ.rowType }")

    val localChildRowType = prevRVD.rowPType
    val keyIndices = prev.typ.keyFieldIdx
    val keyOrd = prevRVD.typ.kRowOrd
    val globalsBc = prev.globals.broadcast

    val newRVDType = prevRVD.typ.copy(rowType = rowType)

    val newRVD = prevRVD
      .repartition(prevRVD.partitioner.strictify, ctx)
      .boundary
      .mapPartitionsWithIndex(newRVDType) { (i, ctx, it) =>
        val partRegion = ctx.partitionRegion
        val globalsOff = globalsBc.value.readRegionValue(partRegion)

        val initialize = makeInit(i, partRegion)
        val sequence = makeSeq(i, partRegion)
        val newRowF = makeRow(i, partRegion)

        val aggRegion = ctx.freshRegion

        new Iterator[Long] {
          var isEnd = false
          var current: Long = 0
          val rowKey: WritableRegionValue = WritableRegionValue(keyType, ctx.freshRegion)
          val consumerRegion: Region = ctx.region
          val newRV = RegionValue(consumerRegion)

          def hasNext: Boolean = {
            if (isEnd || (current == 0 && !it.hasNext)) {
              isEnd = true
              return false
            }
            if (current == 0)
              current = it.next()
            true
          }

          def next(): Long = {
            if (!hasNext)
              throw new java.util.NoSuchElementException()

            rowKey.setSelect(localChildRowType, keyIndices, current, true)

            aggRegion.clear()
            initialize.newAggState(aggRegion)
            initialize(ctx.r, globalsOff)
            sequence.setAggState(aggRegion, initialize.getAggOffset())

            do {
              sequence(ctx.r,
                globalsOff,
                current)
              current = 0
            } while (hasNext && keyOrd.equiv(rowKey.value.offset, current))
            newRowF.setAggState(aggRegion, sequence.getAggOffset())

            newRowF(consumerRegion, globalsOff, rowKey.offset)
          }
        }
      }

    prev.copy(rvd = newRVD, typ = typ)
  }
}

object TableOrderBy {
  def isAlreadyOrdered(sortFields: IndexedSeq[SortField], prevKey: IndexedSeq[String]): Boolean = {
    sortFields.length <= prevKey.length &&
      sortFields.zip(prevKey).forall { case (sf, k) =>
        sf.sortOrder == Ascending && sf.field == k
      }
  }
}

case class TableOrderBy(child: TableIR, sortFields: IndexedSeq[SortField]) extends TableIR {
  // TableOrderBy expects an unkeyed child, so that we can better optimize by
  // pushing these two steps around as needed

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  val children: IndexedSeq[BaseIR] = FastIndexedSeq(child)

  def copy(newChildren: IndexedSeq[BaseIR]): TableOrderBy = {
    val IndexedSeq(newChild) = newChildren
    TableOrderBy(newChild.asInstanceOf[TableIR], sortFields)
  }

  val typ: TableType = child.typ.copy(key = FastIndexedSeq())

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val prev = child.execute(ctx)

    val physicalKey = prev.rvd.typ.key
    if (TableOrderBy.isAlreadyOrdered(sortFields, physicalKey))
      return prev.copy(typ = typ)

    val rowType = child.typ.rowType
    val sortColIndexOrd = sortFields.map { case SortField(n, so) =>
      val i = rowType.fieldIdx(n)
      val f = rowType.fields(i)
      val fo = f.typ.ordering
      if (so == Ascending) fo else fo.reverse
    }.toArray

    val ord: Ordering[Annotation] = ExtendedOrdering.rowOrdering(sortColIndexOrd).toOrdering

    val act = implicitly[ClassTag[Annotation]]

    val codec = TypedCodecSpec(prev.rvd.rowPType, BufferSpec.wireSpec)
    val rdd = prev.rvd.keyedEncodedRDD(codec, sortFields.map(_.field)).sortBy(_._1)(ord, act)
    val (rowPType: PStruct, orderedCRDD) = codec.decodeRDD(rowType, rdd.map(_._2))
    TableValue(typ, prev.globals, RVD.unkeyed(rowPType, orderedCRDD))
  }
}

/** Create a Table from a MatrixTable, storing the column values in a global
  * field 'colsFieldName', and storing the entry values in a row field
  * 'entriesFieldName'.
  */
case class CastMatrixToTable(
  child: MatrixIR,
  entriesFieldName: String,
  colsFieldName: String
) extends TableIR {

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  lazy val typ: TableType = child.typ.toTableType(entriesFieldName, colsFieldName)

  lazy val children: IndexedSeq[BaseIR] = FastIndexedSeq(child)

  def copy(newChildren: IndexedSeq[BaseIR]): CastMatrixToTable = {
    val IndexedSeq(newChild) = newChildren
    CastMatrixToTable(newChild.asInstanceOf[MatrixIR], entriesFieldName, colsFieldName)
  }

  override def partitionCounts: Option[IndexedSeq[Long]] = child.partitionCounts
}

case class TableRename(child: TableIR, rowMap: Map[String, String], globalMap: Map[String, String]) extends TableIR {
  require(rowMap.keys.forall(child.typ.rowType.hasField))
  require(globalMap.keys.forall(child.typ.globalType.hasField))

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  def rowF(old: String): String = rowMap.getOrElse(old, old)

  def globalF(old: String): String = globalMap.getOrElse(old, old)

  lazy val typ: TableType = child.typ.copy(
    rowType = child.typ.rowType.rename(rowMap),
    globalType = child.typ.globalType.rename(globalMap),
    key = child.typ.key.map(k => rowMap.getOrElse(k, k))
  )

  override def partitionCounts: Option[IndexedSeq[Long]] = child.partitionCounts

  lazy val children: IndexedSeq[BaseIR] = FastIndexedSeq(child)

  def copy(newChildren: IndexedSeq[BaseIR]): TableRename = {
    val IndexedSeq(newChild: TableIR) = newChildren
    TableRename(newChild, rowMap, globalMap)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = child.execute(ctx).rename(globalMap, rowMap)
}

case class TableFilterIntervals(child: TableIR, intervals: IndexedSeq[Interval], keep: Boolean) extends TableIR {
  lazy val children: IndexedSeq[BaseIR] = Array(child)

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  def copy(newChildren: IndexedSeq[BaseIR]): TableIR = {
    val IndexedSeq(newChild: TableIR) = newChildren
    TableFilterIntervals(newChild, intervals, keep)
  }

  override lazy val typ: TableType = child.typ

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val tv = child.execute(ctx)
    val partitioner = RVDPartitioner.union(
      tv.typ.keyType,
      intervals,
      tv.rvd.typ.key.length - 1)
    TableValue(tv.typ, tv.globals, tv.rvd.filterIntervals(partitioner, keep))
  }
}

case class TableGroupWithinPartitions(child: TableIR, n: Int) extends TableIR {
  lazy val children: IndexedSeq[BaseIR] = Array(child)

  lazy val rowCountUpperBound: Option[Long] = child.rowCountUpperBound

  override lazy val typ: TableType = TableType(
    child.typ.keyType ++ TStruct(("grouped_fields", TArray(child.typ.rowType))),
    child.typ.key,
    child.typ.globalType)

  def copy(newChildren: IndexedSeq[BaseIR]): TableIR = {
    val IndexedSeq(newChild: TableIR) = newChildren
    TableGroupWithinPartitions(newChild, n)
  }

  override def execute(ctx: ExecuteContext): TableValue = {
    val prev = child.execute(ctx)
    val prevRVD = prev.rvd
    val prevRowType = prev.rvd.typ.rowType

    val rowType = PCanonicalStruct(required = true,
      prev.rvd.typ.kType.fields.map(f => (f.name, f.typ)) ++
        Array(("grouped_fields", PCanonicalArray(prevRowType, required = true))): _*)
    val newRVDType = prevRVD.typ.copy(rowType = rowType)
    val keyIndices = child.typ.keyFieldIdx

    val blockSize = n
    val newRVD = prevRVD.mapPartitions(newRVDType) { (ctx, it) =>
      val rvb = ctx.rvb

      new Iterator[Long] {
        override def hasNext: Boolean = {
          it.hasNext
        }

        override def next(): Long = {
          if (!hasNext)
            throw new java.util.NoSuchElementException()

          val offsetArray = new Array[Long](blockSize) // May be longer than the amount of data
          var childIterationCount = 0
          while (it.hasNext && childIterationCount != blockSize) {
            val nextPtr = it.next()
            offsetArray(childIterationCount) = nextPtr
            childIterationCount += 1
          }
          rvb.start(rowType)
          rvb.startStruct()
          rvb.addFields(prevRowType, ctx.region, offsetArray(0), keyIndices)
          rvb.startArray(childIterationCount, true)
          (0 until childIterationCount) foreach { rvArrayIndex =>
            rvb.addRegionValue(prevRowType, ctx.region, offsetArray(rvArrayIndex))
          }
          rvb.endArray()
          rvb.endStruct()
          rvb.end()
        }
      }
    }

    prev.copy(rvd = newRVD, typ = typ)
  }
}

case class MatrixToTableApply(child: MatrixIR, function: MatrixToTableFunction) extends TableIR {
  lazy val children: IndexedSeq[BaseIR] = Array(child)

  lazy val rowCountUpperBound: Option[Long] = if (function.preservesPartitionCounts) child.rowCountUpperBound else None

  def copy(newChildren: IndexedSeq[BaseIR]): TableIR = {
    val IndexedSeq(newChild: MatrixIR) = newChildren
    MatrixToTableApply(newChild, function)
  }

  override lazy val typ: TableType = function.typ(child.typ)

  override def partitionCounts: Option[IndexedSeq[Long]] =
    if (function.preservesPartitionCounts) child.partitionCounts else None
}

case class TableToTableApply(child: TableIR, function: TableToTableFunction) extends TableIR {
  lazy val children: IndexedSeq[BaseIR] = Array(child)

  def copy(newChildren: IndexedSeq[BaseIR]): TableIR = {
    val IndexedSeq(newChild: TableIR) = newChildren
    TableToTableApply(newChild, function)
  }

  override lazy val typ: TableType = function.typ(child.typ)

  override def partitionCounts: Option[IndexedSeq[Long]] =
    if (function.preservesPartitionCounts) child.partitionCounts else None

  lazy val rowCountUpperBound: Option[Long] = if (function.preservesPartitionCounts) child.rowCountUpperBound else None

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    function.execute(ctx, child.execute(ctx))
  }
}

case class BlockMatrixToTableApply(
  bm: BlockMatrixIR,
  aux: IR,
  function: BlockMatrixToTableFunction) extends TableIR {

  override lazy val children: IndexedSeq[BaseIR] = Array(bm, aux)

  lazy val rowCountUpperBound: Option[Long] = None

  override def copy(newChildren: IndexedSeq[BaseIR]): TableIR =
    BlockMatrixToTableApply(
      newChildren(0).asInstanceOf[BlockMatrixIR],
      newChildren(1).asInstanceOf[IR],
      function)

  override lazy val typ: TableType = function.typ(bm.typ, aux.typ)

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    val b = bm.execute(ctx)
    val a = CompileAndEvaluate[Any](ctx, aux, optimize = false)
    function.execute(ctx, b, a)
  }
}

case class BlockMatrixToTable(child: BlockMatrixIR) extends TableIR {
  lazy val children: IndexedSeq[BaseIR] = Array(child)

  lazy val rowCountUpperBound: Option[Long] = None

  def copy(newChildren: IndexedSeq[BaseIR]): TableIR = {
    val IndexedSeq(newChild: BlockMatrixIR) = newChildren
    BlockMatrixToTable(newChild)
  }

  override val typ: TableType = {
    val rvType = TStruct("i" -> TInt64, "j" -> TInt64, "entry" -> TFloat64)
    TableType(rvType, Array[String](), TStruct.empty)
  }

  protected[ir] override def execute(ctx: ExecuteContext): TableValue = {
    child.execute(ctx).entriesTable(ctx)
  }
}

case class RelationalLetTable(name: String, value: IR, body: TableIR) extends TableIR {
  def typ: TableType = body.typ

  lazy val rowCountUpperBound: Option[Long] = body.rowCountUpperBound

  def children: IndexedSeq[BaseIR] = Array(value, body)

  def copy(newChildren: IndexedSeq[BaseIR]): TableIR = {
    val IndexedSeq(newValue: IR, newBody: TableIR) = newChildren
    RelationalLetTable(name, newValue, newBody)
  }
}
