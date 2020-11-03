package is.hail.backend.local

import java.io.PrintWriter

import is.hail.annotations.{Region, SafeRow, UnsafeRow}
import is.hail.asm4s._
import is.hail.backend.{Backend, BackendContext, BroadcastValue}
import is.hail.expr.ir.lowering._
import is.hail.expr.ir.{IRParser, _}
import is.hail.expr.{JSONAnnotationImpex, Validate}
import is.hail.io.bgen.IndexBgen
import is.hail.io.fs.{FS, HadoopFS}
import is.hail.io.plink.LoadPlink
import is.hail.io.{BufferSpec, TypedCodecSpec}
import is.hail.linalg.BlockMatrix
import is.hail.types.BlockMatrixType
import is.hail.types.encoded.EType
import is.hail.types.physical.{PTuple, PType, PVoid}
import is.hail.types.virtual.TVoid
import is.hail.utils._
import is.hail.variant.ReferenceGenome
import org.apache.hadoop
import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class LocalBroadcastValue[T](val value: T) extends BroadcastValue[T] with Serializable

object LocalBackend {
  private var theLocalBackend: LocalBackend = _

  def apply(tmpdir: String): LocalBackend = synchronized {
    require(theLocalBackend == null)

    theLocalBackend = new LocalBackend(tmpdir)
    theLocalBackend
  }

  def stop(): Unit = synchronized {
    if (theLocalBackend != null) {
      theLocalBackend = null
    }
  }
}

class LocalBackend(
  val tmpdir: String
) extends Backend {
  // FIXME don't rely on hadoop
  val hadoopConf = new hadoop.conf.Configuration()
  hadoopConf.set(
    "hadoop.io.compression.codecs",
    "org.apache.hadoop.io.compress.DefaultCodec,"
      + "is.hail.io.compress.BGzipCodec,"
      + "is.hail.io.compress.BGzipCodecTbi,"
      + "org.apache.hadoop.io.compress.GzipCodec")
  
  val fs: FS = new HadoopFS(new SerializableHadoopConfiguration(hadoopConf))

  def withExecuteContext[T](timer: ExecutionTimer)(f: ExecuteContext => T): T = {
    ExecuteContext.scoped(tmpdir, tmpdir, this, fs, timer)(f)
  }

  def broadcast[T : ClassTag](value: T): BroadcastValue[T] = new LocalBroadcastValue[T](value)

  def parallelizeAndComputeWithIndex(backendContext: BackendContext, collection: Array[Array[Byte]],
    dependency: Option[TableStageDependency] = None)(f: (Array[Byte], Int) => Array[Byte]): Array[Array[Byte]] = {
    collection.zipWithIndex.map { case (c, i) =>
      f(c, i)
    }
  }

  def defaultParallelism: Int = 1

  def stop(): Unit = LocalBackend.stop()

  def executeJSON(ir: IR): String = {
    val (jsonValue, timer) = ExecutionTimer.time("LocalBackend.executeJSON") { timer =>
      val t = ir.typ
      withExecuteContext(timer) { ctx =>
        val value = Pass2.executeUnsafe(ctx, ir)
        JsonMethods.compact(JSONAnnotationImpex.exportAnnotation(value, t))
      }
    }
    timer.logInfo()
    Serialization.write(Map("value" -> jsonValue, "timings" -> timer.toMap))(new DefaultFormats {})
  }

  def executeLiteral(ir: IR): IR = {
    ExecutionTimer.logTime("LocalBackend.executeLiteral") { timer =>
      withExecuteContext(timer) { ctx =>
        val t = ir.typ
        assert(t.isRealizable)
        Literal.coerce(t, Pass2.executeSafe(ctx, ir))
      }
    }
  }

  def encodeToBytes(ir: IR, bufferSpecString: String): (String, Array[Byte]) = {
    ExecutionTimer.logTime("LocalBackend.encodeToBytes") { timer =>
      val bs = BufferSpec.parseOrDefault(bufferSpecString)
      withExecuteContext(timer) { ctx =>
        assert(ir.typ != TVoid)
        val (pt: PTuple, a) = Pass2.executeRaw(ctx, ir)
        assert(pt.size == 1)
        val elementType = pt.fields(0).typ
        val codec = TypedCodecSpec(
          EType.defaultFromPType(elementType), elementType.virtualType, bs)
        assert(pt.isFieldDefined(a, 0))
        (elementType.toString, codec.encode(ctx, elementType, pt.loadField(a, 0)))
      }
    }
  }

  def decodeToJSON(ptypeString: String, b: Array[Byte], bufferSpecString: String): String = {
    ExecutionTimer.logTime("LocalBackend.decodeToJSON") { timer =>
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
    ExecutionTimer.logTime("LocalBackend.pyIndexBgen") { timer =>
      withExecuteContext(timer) { ctx =>
        IndexBgen(ctx, files.asScala.toArray, indexFileMap.asScala.toMap, Option(rg), contigRecoding.asScala.toMap, skipInvalidLoci)
      }
      info(s"Number of BGEN files indexed: ${ files.size() }")
    }
  }

  def pyReferenceAddLiftover(name: String, chainFile: String, destRGName: String): Unit = {
    ExecutionTimer.logTime("LocalBackend.pyReferenceAddLiftover") { timer =>
      withExecuteContext(timer) { ctx =>
        ReferenceGenome.referenceAddLiftover(ctx, name, chainFile, destRGName)
      }
    }
  }

  def pyFromFASTAFile(name: String, fastaFile: String, indexFile: String,
    xContigs: java.util.List[String], yContigs: java.util.List[String], mtContigs: java.util.List[String],
    parInput: java.util.List[String]): ReferenceGenome = {
    ExecutionTimer.logTime("LocalBackend.pyFromFASTAFile") { timer =>
      withExecuteContext(timer) { ctx =>
        ReferenceGenome.fromFASTAFile(ctx, name, fastaFile, indexFile,
          xContigs.asScala.toArray, yContigs.asScala.toArray, mtContigs.asScala.toArray, parInput.asScala.toArray)
      }
    }
  }

  def pyAddSequence(name: String, fastaFile: String, indexFile: String): Unit = {
    ExecutionTimer.logTime("LocalBackend.pyAddSequence") { timer =>
      withExecuteContext(timer) { ctx =>
        ReferenceGenome.addSequence(ctx, name, fastaFile, indexFile)
      }
    }
  }

  def parse_value_ir(s: String, refMap: java.util.Map[String, String], irMap: java.util.Map[String, BaseIR]): IR = {
    ExecutionTimer.logTime("LocalBackend.parse_value_ir") { timer =>
      withExecuteContext(timer) { ctx =>
        IRParser.parse_value_ir(s, IRParserEnvironment(ctx, refMap.asScala.toMap.mapValues(IRParser.parseType), irMap.asScala.toMap))
      }
    }
  }

  def parse_table_ir(s: String, refMap: java.util.Map[String, String], irMap: java.util.Map[String, BaseIR]): TableIR = {
    ExecutionTimer.logTime("LocalBackend.parse_table_ir") { timer =>
      withExecuteContext(timer) { ctx =>
        IRParser.parse_table_ir(s, IRParserEnvironment(ctx, refMap.asScala.toMap.mapValues(IRParser.parseType), irMap.asScala.toMap))
      }
    }
  }

  def parse_matrix_ir(s: String, refMap: java.util.Map[String, String], irMap: java.util.Map[String, BaseIR]): MatrixIR = {
    ExecutionTimer.logTime("LocalBackend.parse_matrix_ir") { timer =>
      withExecuteContext(timer) { ctx =>
        IRParser.parse_matrix_ir(s, IRParserEnvironment(ctx, refMap.asScala.toMap.mapValues(IRParser.parseType), irMap.asScala.toMap))
      }
    }
  }

  def parse_blockmatrix_ir(
    s: String, refMap: java.util.Map[String, String], irMap: java.util.Map[String, BaseIR]
  ): BlockMatrixIR = {
    ExecutionTimer.logTime("LocalBackend.parse_blockmatrix_ir") { timer =>
      withExecuteContext(timer) { ctx =>
        IRParser.parse_blockmatrix_ir(s, IRParserEnvironment(ctx, refMap.asScala.toMap.mapValues(IRParser.parseType), irMap.asScala.toMap))
      }
    }
  }

  def lowerDistributedSort(ctx: ExecuteContext, stage: TableStage, sortFields: IndexedSeq[SortField], relationalLetsAbove: Map[String, IR]): TableStage = {
    // Use a local sort for the moment to enable larger pipelines to run
    LowerDistributedSort.localSort(ctx, stage, sortFields, relationalLetsAbove)
  }

  def pyLoadReferencesFromDataset(path: String): String =
    ReferenceGenome.fromHailDataset(fs, path)

  def pyImportFam(path: String, isQuantPheno: Boolean, delimiter: String, missingValue: String): String =
    LoadPlink.importFamJSON(fs, path, isQuantPheno, delimiter, missingValue)

  def persist(backendContext: BackendContext, id: String, value: BlockMatrix, storageLevel: String): Unit = ???

  def unpersist(backendContext: BackendContext, id: String): Unit = ???

  def getPersistedBlockMatrix(backendContext: BackendContext, id: String): BlockMatrix = ???

  def getPersistedBlockMatrixType(backendContext: BackendContext, id: String): BlockMatrixType = ???
}
