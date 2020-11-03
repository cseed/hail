package is.hail.backend.spark

import is.hail.annotations.{Annotation, ExtendedOrdering, Region, SafeRow, UnsafeRow}
import is.hail.asm4s._
import is.hail.expr.ir.IRParser
import is.hail.types.encoded.EType
import is.hail.io.{BufferSpec, TypedCodecSpec}
import is.hail.HailContext
import is.hail.expr.{JSONAnnotationImpex, SparkAnnotationImpex, Validate}
import is.hail.expr.ir.lowering._
import is.hail.expr.ir._
import is.hail.types.physical.{PStruct, PTuple, PType}
import is.hail.types.virtual.{TStruct, TVoid, Type}
import is.hail.backend.{Backend, BackendContext, BroadcastValue, HailTaskContext}
import is.hail.io.fs.HadoopFS
import is.hail.utils._
import is.hail.io.bgen.IndexBgen
import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}
import org.apache.spark.{Dependency, NarrowDependency, Partition, ProgressBarBuilder, ShuffleDependency, SparkConf, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

import is.hail.io.plink.LoadPlink
import is.hail.io.vcf.VCFsReader
import is.hail.linalg.{BlockMatrix, RowMatrix}
import is.hail.rvd.RVD
import is.hail.stats.LinearMixedModel
import is.hail.types.BlockMatrixType
import is.hail.variant.ReferenceGenome
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.json4s.JsonAST.{JInt, JObject}


class SparkBroadcastValue[T](bc: Broadcast[T]) extends BroadcastValue[T] with Serializable {
  def value: T = bc.value
}

class SparkTaskContext(ctx: TaskContext) extends HailTaskContext {
  type BackendType = SparkBackend
  override def stageId(): Int = ctx.stageId()
  override def partitionId(): Int = ctx.partitionId()
  override def attemptNumber(): Int = ctx.attemptNumber()
}

object SparkBackend {
  private var theSparkBackend: SparkBackend = _

  def sparkContext(op: String): SparkContext = HailContext.sparkBackend(op).sc

  def checkSparkCompatibility(jarVersion: String, sparkVersion: String): Unit = {
    def majorMinor(version: String): String = version.split("\\.", 3).take(2).mkString(".")

    if (majorMinor(jarVersion) != majorMinor(sparkVersion))
      fatal(s"This Hail JAR was compiled for Spark $jarVersion, cannot run with Spark $sparkVersion.\n" +
        s"  The major and minor versions must agree, though the patch version can differ.")
    else if (jarVersion != sparkVersion)
      warn(s"This Hail JAR was compiled for Spark $jarVersion, running with Spark $sparkVersion.\n" +
        s"  Compatibility is not guaranteed.")
  }

  def createSparkConf(appName: String, master: String,
    local: String, blockSize: Long): SparkConf = {
    require(blockSize >= 0)
    checkSparkCompatibility(is.hail.HAIL_SPARK_VERSION, org.apache.spark.SPARK_VERSION)

    val conf = new SparkConf().setAppName(appName)

    if (master != null)
      conf.setMaster(master)
    else {
      if (!conf.contains("spark.master"))
        conf.setMaster(local)
    }

    conf.set("spark.logConf", "true")
    conf.set("spark.ui.showConsoleProgress", "false")

    conf.set("spark.kryoserializer.buffer.max", "1g")
    conf.set("spark.driver.maxResultSize", "0")

    conf.set(
      "spark.hadoop.io.compression.codecs",
      "org.apache.hadoop.io.compress.DefaultCodec," +
        "is.hail.io.compress.BGzipCodec," +
        "is.hail.io.compress.BGzipCodecTbi," +
        "org.apache.hadoop.io.compress.GzipCodec")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator")

    conf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", (blockSize * 1024L * 1024L).toString)

    // load additional Spark properties from HAIL_SPARK_PROPERTIES
    val hailSparkProperties = System.getenv("HAIL_SPARK_PROPERTIES")
    if (hailSparkProperties != null) {
      hailSparkProperties
        .split(",")
        .foreach { p =>
          p.split("=") match {
            case Array(k, v) =>
              log.info(s"set Spark property from HAIL_SPARK_PROPERTIES: $k=$v")
              conf.set(k, v)
            case _ =>
              warn(s"invalid key-value property pair in HAIL_SPARK_PROPERTIES: $p")
          }
        }
    }
    conf
  }

  def configureAndCreateSparkContext(appName: String, master: String,
    local: String, blockSize: Long): SparkContext = {
    new SparkContext(createSparkConf(appName, master, local, blockSize))
  }

  def checkSparkConfiguration(sc: SparkContext) {
    val conf = sc.getConf

    val problems = new mutable.ArrayBuffer[String]

    val serializer = conf.getOption("spark.serializer")
    val kryoSerializer = "org.apache.spark.serializer.KryoSerializer"
    if (!serializer.contains(kryoSerializer))
      problems += s"Invalid configuration property spark.serializer: required $kryoSerializer.  " +
        s"Found: ${ serializer.getOrElse("empty parameter") }."

    if (!conf.getOption("spark.kryo.registrator").exists(_.split(",").contains("is.hail.kryo.HailKryoRegistrator")))
      problems += s"Invalid config parameter: spark.kryo.registrator must include is.hail.kryo.HailKryoRegistrator." +
        s"Found ${ conf.getOption("spark.kryo.registrator").getOrElse("empty parameter.") }"

    if (problems.nonEmpty)
      fatal(
        s"""Found problems with SparkContext configuration:
           |  ${ problems.mkString("\n  ") }""".stripMargin)
  }

  def hailCompressionCodecs: Array[String] = Array(
    "org.apache.hadoop.io.compress.DefaultCodec",
    "is.hail.io.compress.BGzipCodec",
    "is.hail.io.compress.BGzipCodecTbi",
    "org.apache.hadoop.io.compress.GzipCodec")

  /**
    * If a SparkBackend has already been initialized, this function returns it regardless of the
    * parameters with which it was initialized.
    *
    * Otherwise, it initializes and returns a new HailContext.
    */
  def getOrCreate(sc: SparkContext = null,
    appName: String = "Hail",
    master: String = null,
    local: String = "local[*]",
    quiet: Boolean = false,
    minBlockSize: Long = 1L,
    tmpdir: String = "/tmp",
    localTmpdir: String = "file:///tmp"): SparkBackend = synchronized {
    if (theSparkBackend == null)
      return SparkBackend(sc, appName, master, local, quiet, minBlockSize, tmpdir, localTmpdir)

    // there should be only one SparkContext
    assert(sc == null || (sc eq theSparkBackend.sc))

    val initializedMinBlockSize =
      theSparkBackend.sc.getConf.getLong("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", 0L) / 1024L / 1024L
    if (minBlockSize != initializedMinBlockSize)
      warn(s"Requested minBlockSize $minBlockSize, but already initialized to $initializedMinBlockSize.  Ignoring requested setting.")

    if (master != null) {
      val initializedMaster = theSparkBackend.sc.master
      if (master != initializedMaster)
        warn(s"Requested master $master, but already initialized to $initializedMaster.  Ignoring requested setting.")
    }

    theSparkBackend
  }

  def apply(sc: SparkContext = null,
    appName: String = "Hail",
    master: String = null,
    local: String = "local[*]",
    quiet: Boolean = false,
    minBlockSize: Long = 1L,
    tmpdir: String,
    localTmpdir: String): SparkBackend = synchronized {
    require(theSparkBackend == null)

    var sc1 = sc
    if (sc1 == null)
      sc1 = configureAndCreateSparkContext(appName, master, local, minBlockSize)

    sc1.hadoopConfiguration.set("io.compression.codecs", hailCompressionCodecs.mkString(","))

    checkSparkConfiguration(sc1)

    if (!quiet)
      ProgressBarBuilder.build(sc1)

    sc1.uiWebUrl.foreach(ui => info(s"SparkUI: $ui"))

    theSparkBackend = new SparkBackend(tmpdir, localTmpdir, sc1)
    theSparkBackend
  }

  def stop(): Unit = synchronized {
    if (theSparkBackend != null) {
      theSparkBackend.sc.stop()
      theSparkBackend = null
    }
  }
}

// This indicates a narrow (non-shuffle) dependency on _rdd. It works since narrow dependency `getParents`
// is only used to compute preferred locations, which is something we don't need to worry about
class AnonymousDependency[T](val _rdd: RDD[T]) extends NarrowDependency[T](_rdd) {
  override def getParents(partitionId: Int): Seq[Int] = Seq.empty
}

class SparkBackend(
  val tmpdir: String,
  val localTmpdir: String,
  val sc: SparkContext
) extends Backend {
  lazy val sparkSession: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()

  val fs: HadoopFS = new HadoopFS(new SerializableHadoopConfiguration(sc.hadoopConfiguration))

  val bmCache: SparkBlockMatrixCache = SparkBlockMatrixCache()

  def persist(backendContext: BackendContext, id: String, value: BlockMatrix, storageLevel: String): Unit = bmCache.persistBlockMatrix(id, value, storageLevel)

  def unpersist(backendContext: BackendContext, id: String): Unit = unpersist(id)

  def getPersistedBlockMatrix(backendContext: BackendContext, id: String): BlockMatrix = bmCache.getPersistedBlockMatrix(id)

  def getPersistedBlockMatrixType(backendContext: BackendContext, id: String): BlockMatrixType = bmCache.getPersistedBlockMatrixType(id)

  def unpersist(id: String): Unit = bmCache.unpersistBlockMatrix(id)

  def withExecuteContext[T](timer: ExecutionTimer)(f: ExecuteContext => T): T = {
    ExecuteContext.scoped(tmpdir, localTmpdir, this, fs, timer)(f)
  }

  def broadcast[T : ClassTag](value: T): BroadcastValue[T] = new SparkBroadcastValue[T](sc.broadcast(value))

  def parallelizeAndComputeWithIndex(backendContext: BackendContext, collection: Array[Array[Byte]],
    dependency: Option[TableStageDependency] = None)(f: (Array[Byte], Int) => Array[Byte]): Array[Array[Byte]] = {

    val sparkDeps = dependency.toIndexedSeq
      .flatMap(dep => dep.deps.map(rvdDep => new AnonymousDependency(rvdDep.asInstanceOf[RVDDependency].rvd.crdd.rdd)))

    new SparkBackendComputeRDD(sc, collection, f, sparkDeps).collect()
  }

  def defaultParallelism: Int = sc.defaultParallelism

  override def asSpark(op: String): SparkBackend = this

  def stop(): Unit = SparkBackend.stop()

  def startProgressBar() {
    ProgressBarBuilder.build(sc)
  }

  def safeExecute(ir: IR): Any = {
    assert(ir.typ.isRealizable)
    ExecutionTimer.logTime("SparkBackend.safeExecute") { timer =>
      withExecuteContext(timer) { ctx =>
        val RawValue(pt, a) = Pass2.compile.runAny(ctx, ir)
        SafeRow(pt, a).get(0)
      }
    }
  }

  def executeLiteral(ir: IR): IR = {
    assert(ir.typ.isRealizable)
    ExecutionTimer.logTime("SparkBackend.executeLiteral") { timer =>
      withExecuteContext(timer) { ctx =>
        val RawValue(pt, a) = Pass2.compile.runAny(ctx, ir)
        GetFieldByIdx(EncodedLiteral.hailValueToByteArray(pt, a, ctx), 0)
      }
    }
  }
  
  def executeJSON(ir: IR): String = {
    assert(ir.typ.isRealizable)
    val (jsonValue, timer) = ExecutionTimer.time("SparkBackend.executeJSON") { timer =>
      withExecuteContext(timer) { ctx =>
        val RawValue(pt, a) = Pass2.compile.runAny(ctx, ir)
        JsonMethods.compact(JSONAnnotationImpex.exportAnnotation(
          new UnsafeRow(pt, null, a).get(0), pt.virtualType.fields(0).typ))
      }
    }
    Serialization.write(Map("value" -> jsonValue, "timings" -> timer.toMap))(new DefaultFormats {})
  }

  // Called from python
  def encodeToBytes(ir: IR, bufferSpecString: String): (String, Array[Byte]) = {
    ExecutionTimer.logTime("SparkBackend.encodeToBytes") { timer =>
      val bs = BufferSpec.parseOrDefault(bufferSpecString)
      withExecuteContext(timer) { ctx =>
        val RawValue(pt, a) = Pass2.compile.runAny(ctx, ir)
        val elementType = pt.fields(0).typ
        val codec = TypedCodecSpec(
          EType.defaultFromPType(elementType), elementType.virtualType, bs)
        assert(pt.isFieldDefined(a, 0))
        (elementType.toString, codec.encode(ctx, elementType, pt.loadField(a, 0)))
      }
    }
  }

  def decodeToJSON(ptypeString: String, b: Array[Byte], bufferSpecString: String): String = {
    ExecutionTimer.logTime("SparkBackend.decodeToJSON") { timer =>
      val t = IRParser.parsePType(ptypeString)
      val bs = BufferSpec.parseOrDefault(bufferSpecString)
      val codec = TypedCodecSpec(EType.defaultFromPType(t), t.virtualType, bs)
      withExecuteContext(timer) { ctx =>
        val (pt, off) = codec.decode(ctx, t.virtualType, b, ctx.r)
        assert(pt.virtualType == t.virtualType)
        JsonMethods.compact(JSONAnnotationImpex.exportAnnotation(
          UnsafeRow.read(pt, ctx.r, off), pt.virtualType))
      }
    }
  }

  def pyIndexBgen(
    files: java.util.List[String],
    indexFileMap: java.util.Map[String, String],
    rg: String,
    contigRecoding: java.util.Map[String, String],
    skipInvalidLoci: Boolean) {
    ExecutionTimer.logTime("SparkBackend.pyIndexBgen") { timer =>
      withExecuteContext(timer) { ctx =>
        IndexBgen(ctx, files.asScala.toArray, indexFileMap.asScala.toMap, Option(rg), contigRecoding.asScala.toMap, skipInvalidLoci)
      }
      info(s"Number of BGEN files indexed: ${ files.size() }")
    }
  }

  def pyFromDF(df: DataFrame, jKey: java.util.List[String]): TableIR = {
    ExecutionTimer.logTime("SparkBackend.pyFromDF") { timer =>
      val key = jKey.asScala.toArray.toFastIndexedSeq
      val signature = SparkAnnotationImpex.importType(df.schema).setRequired(true).asInstanceOf[PStruct]
      withExecuteContext(timer) { ctx =>
        TableLiteral(TableValue(ctx, signature.virtualType.asInstanceOf[TStruct], key, df.rdd, Some(signature)))
      }
    }
  }

  def pyPersistMatrix(storageLevel: String, mir: MatrixIR): MatrixIR = {
    ExecutionTimer.logTime("SparkBackend.pyPersistMatrix") { timer =>
      val level = try {
        StorageLevel.fromString(storageLevel)
      } catch {
        case e: IllegalArgumentException =>
          fatal(s"unknown StorageLevel: $storageLevel")
      }

      withExecuteContext(timer) { ctx =>
        val tv = Pass2.executeMatrix(ctx, mir)
        MatrixLiteral(mir.typ, TableLiteral(tv.persist(ctx, level)))
      }
    }
  }

  def pyPersistTable(storageLevel: String, tir: TableIR): TableIR = {
    ExecutionTimer.logTime("SparkBackend.pyPersistTable") { timer =>
      val level = try {
        StorageLevel.fromString(storageLevel)
      } catch {
        case e: IllegalArgumentException =>
          fatal(s"unknown StorageLevel: $storageLevel")
      }

      withExecuteContext(timer) { ctx =>
        val tv = Pass2.executeTable(ctx, tir)
        TableLiteral(tv.persist(ctx, level))
      }
    }
  }

  def pyToDF(tir: TableIR): DataFrame = {
    ExecutionTimer.logTime("SparkBackend.pyToDF") { timer =>
      withExecuteContext(timer) { ctx =>
        val tv = Pass2.executeTable(ctx, tir)
        tv.toDF()
      }
    }
  }

  def pyImportVCFs(
    files: java.util.List[String],
    callFields: java.util.List[String],
    entryFloatTypeName: String,
    rg: String,
    contigRecoding: java.util.Map[String, String],
    arrayElementsRequired: Boolean,
    skipInvalidLoci: Boolean,
    gzAsBGZ: Boolean,
    forceGZ: Boolean,
    partitionsJSON: String,
    partitionsTypeStr: String,
    filter: String,
    find: String,
    replace: String,
    externalSampleIds: java.util.List[java.util.List[String]],
    externalHeader: String
  ): String = {
    ExecutionTimer.logTime("SparkBackend.pyImportVCFs") { timer =>
      withExecuteContext(timer) { ctx =>
        val reader = new VCFsReader(ctx,
          files.asScala.toArray,
          callFields.asScala.toSet,
          entryFloatTypeName,
          Option(rg),
          Option(contigRecoding).map(_.asScala.toMap).getOrElse(Map.empty[String, String]),
          arrayElementsRequired,
          skipInvalidLoci,
          gzAsBGZ,
          forceGZ,
          TextInputFilterAndReplace(Option(find), Option(filter), Option(replace)),
          partitionsJSON, partitionsTypeStr,
          Option(externalSampleIds).map(_.asScala.map(_.asScala.toArray).toArray),
          Option(externalHeader))

        val irs = reader.read(ctx)
        val id = HailContext.get.addIrVector(irs)
        val out = JObject(
          "vector_ir_id" -> JInt(id),
          "length" -> JInt(irs.length),
          "type" -> reader.typ.pyJson)
        JsonMethods.compact(out)
      }
    }
  }

  def pyReferenceAddLiftover(name: String, chainFile: String, destRGName: String): Unit = {
    ExecutionTimer.logTime("SparkBackend.pyReferenceAddLiftover") { timer =>
      withExecuteContext(timer) { ctx =>
        ReferenceGenome.referenceAddLiftover(ctx, name, chainFile, destRGName)
      }
    }
  }

  def pyFromFASTAFile(name: String, fastaFile: String, indexFile: String,
    xContigs: java.util.List[String], yContigs: java.util.List[String], mtContigs: java.util.List[String],
    parInput: java.util.List[String]): ReferenceGenome = {
    ExecutionTimer.logTime("SparkBackend.pyFromFASTAFile") { timer =>
      withExecuteContext(timer) { ctx =>
        ReferenceGenome.fromFASTAFile(ctx, name, fastaFile, indexFile,
          xContigs.asScala.toArray, yContigs.asScala.toArray, mtContigs.asScala.toArray, parInput.asScala.toArray)
      }
    }
  }

  def pyAddSequence(name: String, fastaFile: String, indexFile: String): Unit = {
    ExecutionTimer.logTime("SparkBackend.pyAddSequence") { timer =>
      withExecuteContext(timer) { ctx =>
        ReferenceGenome.addSequence(ctx, name, fastaFile, indexFile)
      }
    }
  }

  def pyExportBlockMatrix(
    pathIn: String, pathOut: String, delimiter: String, header: String, addIndex: Boolean, exportType: String,
    partitionSize: java.lang.Integer, entries: String): Unit = {
    ExecutionTimer.logTime("SparkBackend.pyExportBlockMatrix") { timer =>
      withExecuteContext(timer) { ctx =>
        val rm = RowMatrix.readBlockMatrix(fs, pathIn, partitionSize)
        entries match {
          case "full" =>
            rm.export(ctx, pathOut, delimiter, Option(header), addIndex, exportType)
          case "lower" =>
            rm.exportLowerTriangle(ctx, pathOut, delimiter, Option(header), addIndex, exportType)
          case "strict_lower" =>
            rm.exportStrictLowerTriangle(ctx, pathOut, delimiter, Option(header), addIndex, exportType)
          case "upper" =>
            rm.exportUpperTriangle(ctx, pathOut, delimiter, Option(header), addIndex, exportType)
          case "strict_upper" =>
            rm.exportStrictUpperTriangle(ctx, pathOut, delimiter, Option(header), addIndex, exportType)
        }
      }
    }
  }

  def pyFitLinearMixedModel(lmm: LinearMixedModel, pa_t: RowMatrix, a_t: RowMatrix): TableIR = {
    ExecutionTimer.logTime("SparkBackend.pyAddSequence") { timer =>
      withExecuteContext(timer) { ctx =>
        lmm.fit(ctx, pa_t, Option(a_t))
      }
    }
  }

  def parse_value_ir(s: String, refMap: java.util.Map[String, String], irMap: java.util.Map[String, BaseIR]): IR = {
    ExecutionTimer.logTime("SparkBackend.parse_value_ir") { timer =>
      withExecuteContext(timer) { ctx =>
        IRParser.parse_value_ir(s, IRParserEnvironment(ctx, refMap.asScala.toMap.mapValues(IRParser.parseType), irMap.asScala.toMap))
      }
    }
  }

  def parse_table_ir(s: String, refMap: java.util.Map[String, String], irMap: java.util.Map[String, BaseIR]): TableIR = {
    ExecutionTimer.logTime("SparkBackend.parse_table_ir") { timer =>
      withExecuteContext(timer) { ctx =>
        IRParser.parse_table_ir(s, IRParserEnvironment(ctx, refMap.asScala.toMap.mapValues(IRParser.parseType), irMap.asScala.toMap))
      }
    }
  }

  def parse_matrix_ir(s: String, refMap: java.util.Map[String, String], irMap: java.util.Map[String, BaseIR]): MatrixIR = {
    ExecutionTimer.logTime("SparkBackend.parse_matrix_ir") { timer =>
      withExecuteContext(timer) { ctx =>
        IRParser.parse_matrix_ir(s, IRParserEnvironment(ctx, refMap.asScala.toMap.mapValues(IRParser.parseType), irMap.asScala.toMap))
      }
    }
  }

  def parse_blockmatrix_ir(
    s: String, refMap: java.util.Map[String, String], irMap: java.util.Map[String, BaseIR]
  ): BlockMatrixIR = {
    ExecutionTimer.logTime("SparkBackend.parse_blockmatrix_ir") { timer =>
      withExecuteContext(timer) { ctx =>
        IRParser.parse_blockmatrix_ir(s, IRParserEnvironment(ctx, refMap.asScala.toMap.mapValues(IRParser.parseType), irMap.asScala.toMap))
      }
    }
  }

  def lowerDistributedSort(ctx: ExecuteContext, stage: TableStage, sortFields: IndexedSeq[SortField], relationalLetsAbove: Map[String, IR]): TableStage = {
    val (globals, rvd) = TableStageToRVD(ctx, stage, relationalLetsAbove)

    if (sortFields.forall(_.sortOrder == Ascending)) {
      return RVDToTableStage(rvd.changeKey(ctx, sortFields.map(_.field)), globals.toEncodedLiteral())
    }

    val rowType = rvd.rowType
    val sortColIndexOrd = sortFields.map { case SortField(n, so) =>
      val i = rowType.fieldIdx(n)
      val f = rowType.fields(i)
      val fo = f.typ.ordering
      if (so == Ascending) fo else fo.reverse
    }.toArray

    val ord: Ordering[Annotation] = ExtendedOrdering.rowOrdering(sortColIndexOrd).toOrdering

    val act = implicitly[ClassTag[Annotation]]

    val codec = TypedCodecSpec(rvd.rowPType, BufferSpec.wireSpec)
    val rdd = rvd.keyedEncodedRDD(ctx, codec, sortFields.map(_.field)).sortBy(_._1)(ord, act)
    val (rowPType: PStruct, orderedCRDD) = codec.decodeRDD(ctx, rowType, rdd.map(_._2))
    RVDToTableStage(RVD.unkeyed(rowPType, orderedCRDD), globals.toEncodedLiteral())
  }

  def pyImportFam(path: String, isQuantPheno: Boolean, delimiter: String, missingValue: String): String =
    LoadPlink.importFamJSON(fs, path, isQuantPheno, delimiter, missingValue)
}

case class SparkBackendComputeRDDPartition(data: Array[Byte], index: Int) extends Partition

class SparkBackendComputeRDD(
  sc: SparkContext,
  @transient private val collection: Array[Array[Byte]],
  f: (Array[Byte], Int) => Array[Byte],
  deps: Seq[Dependency[_]])
  extends RDD[Array[Byte]](sc, deps) {

  override def getPartitions: Array[Partition] = {
    Array.tabulate(collection.length)(i => SparkBackendComputeRDDPartition(collection(i), i))
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val sp = partition.asInstanceOf[SparkBackendComputeRDDPartition]
    HailTaskContext.setTaskContext(new SparkTaskContext(TaskContext.get))
    Iterator.single(f(sp.data, sp.index))
  }
}
