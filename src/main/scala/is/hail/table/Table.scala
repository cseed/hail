package is.hail.table

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr._
import is.hail.expr.types._
import is.hail.expr.ir._
import is.hail.io.annotators.{BedAnnotator, IntervalList}
import is.hail.io.plink.{FamFileConfig, PlinkLoader}
import is.hail.io.{CassandraConnector, SolrConnector, exportTypes}
import is.hail.methods.{Aggregators, Filter}
import is.hail.rvd.{OrderedRVD, OrderedRVPartitioner, OrderedRVType, RVD}
import is.hail.utils._
import is.hail.variant.{GenomeReference, MatrixTable}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

sealed abstract class SortOrder

case object Ascending extends SortOrder

case object Descending extends SortOrder

object SortColumn {
  implicit def fromColumn(column: String): SortColumn = SortColumn(column, Ascending)
}

case class SortColumn(column: String, sortOrder: SortOrder)

case class TableMetadata(
  file_version: Int,
  hail_version: String,
  key: Array[String],
  table_type: String,
  globals: JValue,
  n_partitions: Int,
  partition_counts: Array[Long])

case class TableLocalValue(globals: Row)

object Table {
  final val fileVersion: Int = 0x101

  def range(hc: HailContext, n: Int, name: String = "index", partitions: Option[Int] = None): Table = {
    val range = Range(0, n).view.map(Row(_))
    val rdd = partitions match {
      case Some(parts) => hc.sc.parallelize(range, numSlices = parts)
      case None => hc.sc.parallelize(range)
    }
    Table(hc, rdd, TStruct(name -> TInt32()), Array(name))
  }

  def fromDF(hc: HailContext, df: DataFrame, key: java.util.ArrayList[String]): Table = {
    fromDF(hc, df, key.asScala.toArray)
  }

  def fromDF(hc: HailContext, df: DataFrame, key: Array[String] = Array.empty[String]): Table = {
    val signature = SparkAnnotationImpex.importType(df.schema).asInstanceOf[TStruct]
    Table(hc, df.rdd.map { r =>
      SparkAnnotationImpex.importAnnotation(r, signature).asInstanceOf[Row]
    },
      signature, key)
  }

  def read(hc: HailContext, path: String): Table = {
    if (!hc.hadoopConf.exists(path))
      fatal(s"$path does not exist")
    else if (!path.endsWith(".kt") && !path.endsWith(".kt/"))
      fatal(s"key table files must end in '.kt', but found '$path'")

    GenomeReference.importReferences(hc.hadoopConf, path + "/references/")

    val metadataFile = path + "/metadata.json.gz"
    if (!hc.hadoopConf.exists(metadataFile))
      fatal(
        s"corrupt Table: metadata file does not exist: $metadataFile")

    val metadata = hc.hadoopConf.readFile(metadataFile) { in =>
      // FIXME why doesn't this work?  Serialization.read[KeyTableMetadata](in)
      val json = parse(in)
      json.extract[TableMetadata]
    }

    val tableType = Parser.parseTableType(metadata.table_type)
    val globals = JSONAnnotationImpex.importAnnotation(metadata.globals, tableType.globalType).asInstanceOf[Row]
    new Table(hc, TableRead(path,
      tableType,
      TableLocalValue(globals),
      dropRows = false,
      metadata.n_partitions,
      Some(metadata.partition_counts)))
  }

  def parallelize(hc: HailContext, rows: java.util.ArrayList[Row], signature: TStruct,
    keyNames: java.util.ArrayList[String], nPartitions: Option[Int]): Table = {
    parallelize(hc, rows.asScala.toArray, signature, keyNames.asScala.toArray, nPartitions)
  }

  def parallelize(hc: HailContext, rows: IndexedSeq[Row], signature: TStruct,
    key: IndexedSeq[String], nPartitions: Option[Int] = None): Table = {
    Table(hc,
      nPartitions match {
        case Some(n) =>
          hc.sc.parallelize(rows, n)
        case None =>
          hc.sc.parallelize(rows)
      }, signature, key.toArray)
  }

  def importIntervalList(hc: HailContext, filename: String,
    gr: GenomeReference = GenomeReference.defaultReference): Table = {
    IntervalList.read(hc, filename, gr)
  }

  def importBED(hc: HailContext, filename: String,
    gr: GenomeReference = GenomeReference.defaultReference): Table = {
    BedAnnotator.apply(hc, filename, gr)
  }

  def importFam(hc: HailContext, path: String, isQuantPheno: Boolean = false,
    delimiter: String = "\\t",
    missingValue: String = "NA"): Table = {

    val ffConfig = FamFileConfig(isQuantPheno, delimiter, missingValue)

    val (data, typ) = PlinkLoader.parseFam(path, ffConfig, hc.hadoopConf)

    val rdd = hc.sc.parallelize(data)

    Table(hc, rdd, typ, Array("id"))
  }

  def apply(hc: HailContext, rdd: RDD[Row], signature: TStruct, key: Array[String] = Array.empty,
    globalSignature: TStruct = TStruct.empty(), globals: Row = Row.empty): Table = {
    val rdd2 = rdd.mapPartitions { it =>
      val region = Region()
      val rvb = new RegionValueBuilder(region)
      val rv = RegionValue(region)
      it.map { row =>
        region.clear()
        rvb.start(signature)
        rvb.addAnnotation(signature, row)
        rv.setOffset(rvb.end())
        rv
      }
    }

    new Table(hc, TableLiteral(
      TableValue(TableType(signature, key, globalSignature),
        TableLocalValue(globals),
        RVD(signature, rdd2))
    ))
  }
}

class Table(val hc: HailContext, val ir: TableIR) {

  def this(hc: HailContext, rdd: RDD[RegionValue], signature: TStruct, key: Array[String] = Array.empty,
    globalSignature: TStruct = TStruct.empty(), globals: Row = Row.empty) = this(hc, TableLiteral(
    TableValue(TableType(signature, key, globalSignature), TableLocalValue(globals), RVD(signature, rdd))
  ))

  lazy val value: TableValue = {
    val opt = TableIR.optimize(ir)
    opt.execute(hc)
  }

  lazy val TableValue(ktType, TableLocalValue(globals), rvd) = value

  val TableType(signature, key, globalSignature) = ir.typ

  lazy val rdd: RDD[Row] = value.rdd

  if (!(fieldNames ++ globalSignature.fieldNames).areDistinct())
    fatal(s"Column names are not distinct: ${ (fieldNames ++ globalSignature.fieldNames).duplicates().mkString(", ") }")
  if (!key.areDistinct())
    fatal(s"Key names are not distinct: ${ key.duplicates().mkString(", ") }")
  if (!key.forall(fieldNames.contains(_)))
    fatal(s"Key names found that are not column names: ${ key.filterNot(fieldNames.contains(_)).mkString(", ") }")

  private def rowEvalContext(): EvalContext = {
    val ec = EvalContext(
      fields.map { f => f.name -> f.typ } ++
        globalSignature.fields.map { f => f.name -> f.typ }: _*)
    var i = 0
    while (i < globalSignature.size) {
      ec.set(i + nColumns, globals.get(i))
      i += 1
    }
    ec
  }

  def fields: Array[Field] = signature.fields.toArray

  val keyFieldIdx: Array[Int] = key.map(signature.fieldIdx)

  def keyFields: Array[Field] = key.map(signature.fieldIdx).map(i => fields(i))

  val valueFieldIdx: Array[Int] = signature.fields.filter(f => !key.contains(f.name)).map(_.index).toArray

  def fieldNames: Array[String] = fields.map(_.name)

  def count(): Long = rvd.count()

  def nColumns: Int = fields.length

  def nKeys: Int = key.length

  def nPartitions: Int = rvd.partitions.length

  def keySignature: TStruct = {
    val (t, _) = signature.select(key)
    t
  }

  def valueSignature: TStruct = {
    val (t, _) = signature.filter(key.toSet, include = false)
    t
  }

  def typeCheck() {

    if (!globalSignature.typeCheck(globals)) {
      fatal(
        s"""found violation of global signature
           |  Schema: ${ globalSignature.toString }
           |  Annotation: $globals""".stripMargin)
    }

    val localSignature = signature
    rdd.foreach { a =>
      if (!localSignature.typeCheck(a))
        fatal(
          s"""found violation in row annotation
             |  Schema: ${ localSignature.toString }
             |
             |  Annotation: ${ Annotation.printAnnotation(a) }""".stripMargin
        )
    }
  }

  def keyedRDD(): RDD[(Row, Row)] = {
    val fieldIndices = fields.map(f => f.name -> f.index).toMap
    val keyIndices = key.map(fieldIndices)
    val keyIndexSet = keyIndices.toSet
    val valueIndices = fields.filter(f => !keyIndexSet.contains(f.index)).map(_.index)
    rdd.map { r => (Row.fromSeq(keyIndices.map(r.get)), Row.fromSeq(valueIndices.map(r.get))) }
  }

  def same(other: Table): Boolean = {
    if (signature != other.signature) {
      info(
        s"""different signatures:
           | left: ${ signature.toString }
           | right: ${ other.signature.toString }
           |""".stripMargin)
      false
    } else if (key.toSeq != other.key.toSeq) {
      info(
        s"""different key names:
           | left: ${ key.mkString(", ") }
           | right: ${ other.key.mkString(", ") }
           |""".stripMargin)
      false
    } else if (globalSignature != other.globalSignature) {
      info(
        s"""different global signatures:
           | left: ${ globalSignature.toString }
           | right: ${ other.globalSignature.toString }
           |""".stripMargin)
      false
    } else if (globals != other.globals) {
      info(
        s"""different global annotations:
           | left: $globals
           | right: ${ other.globals }
           |""".stripMargin)
      false
    } else {
      keyedRDD().groupByKey().fullOuterJoin(other.keyedRDD().groupByKey()).forall { case (k, (v1, v2)) =>
        (v1, v2) match {
          case (None, None) => true
          case (Some(x), Some(y)) =>
            val r1 = x.toArray
            val r2 = y.toArray
            val res = if (r1.length != r2.length)
              false
            else r1.counter() == r2.counter()
            if (!res)
              info(s"SAME KEY, DIFFERENT VALUES: k=$k\n  left:\n    ${ r1.mkString("\n    ") }\n  right:\n    ${ r2.mkString("\n    ") }")
            res
          case _ =>
            info(s"KEY MISMATCH: k=$k\n  left=$v1\n  right=$v2")
            false
        }
      }
    }
  }

  def mapAnnotations[T](f: (Row) => T)(implicit tct: ClassTag[T]): RDD[T] = rdd.map(r => f(r))

  def query(expr: String): (Annotation, Type) = query(Array(expr)).head

  def query(exprs: java.util.ArrayList[String]): Array[(Annotation, Type)] = query(exprs.asScala.toArray)

  def query(exprs: Array[String]): Array[(Annotation, Type)] = {
    val aggregationST = (fields.map { f => f.name -> f.typ } ++
      globalSignature.fields.map { f => f.name -> f.typ })
      .zipWithIndex
      .map { case ((name, t), i) => name -> (i, t) }
      .toMap

    val ec = EvalContext((fields.map { f => f.name -> TAggregable(f.typ, aggregationST) } ++
      globalSignature.fields.map { f => f.name -> f.typ })
      .zipWithIndex
      .map { case ((name, t), i) => name -> (i, t) }
      .toMap)

    val g = globals.asInstanceOf[Row]
    var i = 0
    while (i < globalSignature.size) {
      ec.set(nColumns + i, g.get(i))
      i += 1
    }

    val ts = exprs.map(e => Parser.parseExpr(e, ec))

    val (zVals, seqOp, combOp, resultOp) = Aggregators.makeFunctions[Annotation](ec, {
      case (ec, a) =>
        ec.setAllFromRow(a.asInstanceOf[Row])
    })

    val r = rdd.aggregate(zVals.map(_.copy()))(seqOp, combOp)
    resultOp(r)

    ts.map { case (t, f) => (f(), t) }
  }

  def queryRow(code: String): (Type, Querier) = {
    val ec = rowEvalContext()
    val (t, f) = Parser.parseExpr(code, ec)

    val f2: (Annotation) => Any = { a =>
      ec.setAllFromRow(a.asInstanceOf[Row])
      f()
    }

    (t, f2)
  }

  def annotateGlobal(a: Annotation, t: Type, name: String): Table = {
    val (newT, i) = globalSignature.insert(t, name)
    copy2(globalSignature = newT.asInstanceOf[TStruct], globals = i(globals, a).asInstanceOf[Row])
  }

  def annotateGlobalExpr(expr: String): Table = {
    val ec = EvalContext(globalSignature.fields.map(fd => (fd.name, fd.typ)): _*)

    ec.setAllFromRow(globals.asInstanceOf[Row])
    val (paths, types, f) = Parser.parseAnnotationExprs(expr, ec, None)

    val inserterBuilder = new ArrayBuilder[Inserter]()

    val finalType = (paths, types).zipped.foldLeft(globalSignature) { case (v, (ids, signature)) =>
      val (s, i) = v.insert(signature, ids)
      inserterBuilder += i
      s.asInstanceOf[TStruct]
    }

    val inserters = inserterBuilder.result()

    val ga = inserters
      .zip(f())
      .foldLeft(globals) { case (a, (ins, res)) =>
        ins(a, res).asInstanceOf[Row]
      }

    copy2(globals = ga,
      globalSignature = finalType)
  }

  def selectGlobal(exprs: java.util.ArrayList[String]): Table = {
    val ec = EvalContext(globalSignature.fields.map(fd => (fd.name, fd.typ)): _*)
    ec.setAllFromRow(globals.asInstanceOf[Row])

    val (maybePaths, types, f, isNamedExpr) = Parser.parseSelectExprs(exprs.asScala.toArray, ec)

    val paths = maybePaths.zip(isNamedExpr).map { case (p, isNamed) =>
      if (isNamed)
        List(p.mkString(".")) // FIXME: Not certain what behavior we want here
      else {
        List(p.last)
      }
    }

    val overlappingPaths = paths.counter().filter { case (n, i) => i != 1 }.keys

    if (overlappingPaths.nonEmpty)
      fatal(s"Found ${ overlappingPaths.size } ${ plural(overlappingPaths.size, "selected field name") } that are duplicated.\n" +
        "Overlapping fields:\n  " +
        s"@1", overlappingPaths.truncatable("\n  "))

    val inserterBuilder = new ArrayBuilder[Inserter]()

    val finalSignature = (paths, types).zipped.foldLeft(TStruct()) { case (vs, (p, sig)) =>
      val (s: TStruct, i) = vs.insert(sig, p)
      inserterBuilder += i
      s
    }

    val inserters = inserterBuilder.result()

    val newGlobal = f().zip(inserters)
      .foldLeft(Row()) { case (a1, (v, inserter)) =>
        inserter(a1, v).asInstanceOf[Row]
      }

    copy2(globalSignature = finalSignature, globals = newGlobal)
  }

  def annotate(cond: String): Table = {

    val ec = rowEvalContext()

    val (paths, asts) = Parser.parseAnnotationExprsToAST(cond, ec, None)
    if (paths.length == 0)
      return this

    val irs = asts.flatMap { x => x.typecheck(ec); x.toIR() }

    if (irs.length != asts.length) {
      info("Some ASTs could not be converted to IR. Falling back to AST predicate for Table.annotate.")
      val (paths, types, f) = Parser.parseAnnotationExprs(cond, ec, None)

      val inserterBuilder = new ArrayBuilder[Inserter]()

      val finalSignature = (paths, types).zipped.foldLeft(signature) { case (vs, (ids, sig)) =>
        val (s: TStruct, i) = vs.insert(sig, ids)
        inserterBuilder += i
        s
      }

      val inserters = inserterBuilder.result()

      val annotF: Row => Row = { r =>
        ec.setAllFromRow(r)

        f().zip(inserters)
          .foldLeft(r) { case (a1, (v, inserter)) =>
            inserter(a1, v).asInstanceOf[Row]
          }
      }

      copy(rdd = mapAnnotations(annotF), signature = finalSignature, key = key)
    } else {
      def flatten(old: IR, fields: IndexedSeq[(List[String], IR)]): IndexedSeq[(String, IR)] = {
        if (fields.exists(_._1.isEmpty)) {
          val i = fields.lastIndexWhere(_._1.isEmpty)
          return flatten(old, fields.slice(i, fields.length))
        }
        val flatFields: mutable.Map[String, (mutable.ArrayBuffer[(List[String], IR)])] = mutable.Map()
        for ((path, fIR) <- fields) {
          val name = path.head
          if (!flatFields.contains(name))
            flatFields += ((name, new mutable.ArrayBuffer[(List[String], IR)]()))
          flatFields(name) += ((path.tail, fIR))
        }
        Infer(old)
        flatFields.map { case (f, newF) =>
          if (newF.last._1.isEmpty) {
            (f, newF.last._2)
          } else {
            old.typ match {
              case t: TStruct if t.selfField(f).isDefined =>
                (f, InsertFields(GetField(old, f), flatten(GetField(old, f), newF)))
              case _ =>
                (f, MakeStruct(flatten(I32(0), newF)))
            }
          }
        }.toIndexedSeq
      }

      val (fieldNames, fieldIRs) = flatten(In(0, signature), paths.zip(irs)).unzip
      new Table(hc, TableAnnotate(ir, fieldNames, fieldIRs))
    }


  }

  def filter(cond: String, keep: Boolean): Table = {
    var filterAST = Parser.expr.parse(cond)
    val pred = filterAST.toIR()
    pred match {
      case Some(irPred) =>
        new Table(hc, TableFilter(ir, if (keep) irPred else ApplyUnaryPrimOp(Bang(), irPred)))
      case None =>
        info("No AST to IR conversion found. Falling back to AST predicate for Table.filter.")
        if (!keep)
          filterAST = Apply(filterAST.getPos, "!", Array(filterAST))
        val ec = ktType.rowEC
        val f: () => java.lang.Boolean = Parser.evalTypedExpr[java.lang.Boolean](filterAST, ec)
        val localGlobals = globals
        val localGlobalType = globalSignature
        val localSignature = signature
        var i = 0
        while (i < localGlobalType.size) {
          ec.set(localSignature.size + i, localGlobals.get(i))
          i += 1
        }
        val p = (rv: RegionValue) => {
          val ur = new UnsafeRow(localSignature, rv.region.copy(), rv.offset)

          var i = 0
          while (i < localSignature.size) {
            ec.set(i, ur.get(i))
            i += 1
          }
          val ret = f()
          ret != null && ret.booleanValue()
        }
        copy2(rvd = rvd.filter(p))
    }
  }

  def head(n: Long): Table = {
    if (n < 0)
      fatal(s"n must be non-negative! Found `$n'.")
    copy(rdd = rdd.head(n))
  }

  def keyBy(key: String*): Table = keyBy(key)

  def keyBy(key: java.util.ArrayList[String]): Table = keyBy(key.asScala)

  def keyBy(key: Iterable[String]): Table = {
    val colSet = fieldNames.toSet
    val badKeys = key.filter(!colSet.contains(_))

    if (badKeys.nonEmpty)
      fatal(
        s"""Invalid ${ plural(badKeys.size, "key") }: [ ${ badKeys.map(x => s"'$x'").mkString(", ") } ]
           |  Available columns: [ ${ signature.fields.map(x => s"'${ x.name }'").mkString(", ") } ]""".stripMargin)

    copy(key = key.toArray)
  }

  def select(exprs: Array[String], qualifiedName: Boolean = false): Table = {
    val ec = rowEvalContext()

    val (maybePaths, types, f, isNamedExpr) = Parser.parseSelectExprs(exprs, ec)

    val paths = maybePaths.zip(isNamedExpr).map { case (p, isNamed) =>
      if (isNamed)
        List(p.mkString(".")) // FIXME: Not certain what behavior we want here
      else {
        if (qualifiedName)
          List(p.mkString("."))
        else
          List(p.last)
      }
    }

    val overlappingPaths = paths.counter.filter { case (n, i) => i != 1 }.keys

    if (overlappingPaths.nonEmpty)
      fatal(s"Found ${ overlappingPaths.size } ${ plural(overlappingPaths.size, "selected field name") } that are duplicated.\n" +
        "Either rename manually or use the 'mangle' option to handle duplicates.\n Overlapping fields:\n  " +
        s"@1", overlappingPaths.truncatable("\n  "))

    val inserterBuilder = new ArrayBuilder[Inserter]()

    val finalSignature = (paths, types).zipped.foldLeft(TStruct()) { case (vs, (p, sig)) =>
      val (s: TStruct, i) = vs.insert(sig, p)
      inserterBuilder += i
      s
    }

    val inserters = inserterBuilder.result()

    val annotF: Row => Row = { r =>
      ec.setAllFromRow(r)

      f().zip(inserters)
        .foldLeft(Row()) { case (a1, (v, inserter)) =>
          inserter(a1, v).asInstanceOf[Row]
        }
    }

    val newKey = key.filter(paths.map(_.mkString(".")).toSet)

    copy(rdd = mapAnnotations(annotF), signature = finalSignature, key = newKey)
  }

  def select(selectedColumns: String*): Table = select(selectedColumns.toArray)

  def select(selectedColumns: java.util.ArrayList[String], mangle: Boolean): Table =
    select(selectedColumns.asScala.toArray, mangle)

  def drop(columnsToDrop: Array[String]): Table = {
    if (columnsToDrop.isEmpty)
      return this

    val colMapping = fieldNames.map(c => prettyIdentifier(c) -> c).toMap
    val escapedCols = fieldNames.map(prettyIdentifier)
    val nonexistentColumns = columnsToDrop.diff(escapedCols)
    if (nonexistentColumns.nonEmpty)
      fatal(s"Columns `${ nonexistentColumns.mkString(", ") }' do not exist in key table. Choose from `${ fieldNames.mkString(", ") }'.")

    val selectedColumns = escapedCols.diff(columnsToDrop)
    select(selectedColumns.map(colMapping(_)))
  }

  def drop(columnsToDrop: java.util.ArrayList[String]): Table = drop(columnsToDrop.asScala.toArray)

  def rename(columnMap: Map[String, String]): Table = {
    val newFields = signature.fields.map { fd => fd.copy(name = columnMap.getOrElse(fd.name, fd.name)) }
    val duplicates = newFields.map(_.name).duplicates()
    if (duplicates.nonEmpty)
      fatal(s"Found duplicate column names after renaming columns: `${ duplicates.mkString(", ") }'")

    val newSignature = TStruct(newFields)
    val newColumns = newSignature.fields.map(_.name)
    val newKey = key.map(n => columnMap.getOrElse(n, n))
    val duplicateColumns = newColumns.foldLeft(Map[String, Int]() withDefaultValue 0) { (m, x) => m + (x -> (m(x) + 1)) }.filter {
      _._2 > 1
    }

    copy(rdd = rdd, signature = newSignature, key = newKey)
  }

  def rename(newColumns: Array[String]): Table = {
    if (newColumns.length != nColumns)
      fatal(s"Found ${ newColumns.length } new column names but need $nColumns.")

    rename((fieldNames, newColumns).zipped.toMap)
  }

  def rename(columnMap: java.util.HashMap[String, String]): Table = rename(columnMap.asScala.toMap)

  def rename(newColumns: java.util.ArrayList[String]): Table = rename(newColumns.asScala.toArray)

  def join(other: Table, joinType: String): Table = {
    if (key.length != other.key.length || !(keyFields.map(_.typ) sameElements other.keyFields.map(_.typ)))
      fatal(
        s"""Both key tables must have the same number of keys and the types of keys must be identical. Order matters.
           |  Left signature: ${ keySignature.toString }
           |  Right signature: ${ other.keySignature.toString }""".stripMargin)

    val joinedFields = keySignature.fields ++ valueSignature.fields ++ other.valueSignature.fields

    val preNames = joinedFields.map(_.name).toArray
    val (finalColumnNames, remapped) = mangle(preNames)
    if (remapped.nonEmpty) {
      warn(s"Remapped ${ remapped.length } ${ plural(remapped.length, "column") } from right-hand table:\n  @1",
        remapped.map { case (pre, post) => s""""$pre" => "$post"""" }.truncatable("\n  "))
    }

    val newSignature = TStruct(joinedFields
      .zipWithIndex
      .map { case (fd, i) => (finalColumnNames(i), fd.typ) }: _*)
    val localNKeys = nKeys
    val size1 = valueSignature.size
    val size2 = other.valueSignature.size
    val totalSize = newSignature.size

    assert(totalSize == localNKeys + size1 + size2)

    val merger = (k: Row, r1: Row, r2: Row) => {
      val result = Array.fill[Any](totalSize)(null)

      var i = 0
      while (i < localNKeys) {
        result(i) = k.get(i)
        i += 1
      }

      if (r1 != null) {
        i = 0
        while (i < size1) {
          result(localNKeys + i) = r1.get(i)
          i += 1
        }
      }

      if (r2 != null) {
        i = 0
        while (i < size2) {
          result(localNKeys + size1 + i) = r2.get(i)
          i += 1
        }
      }
      Row.fromSeq(result)
    }

    val rddLeft = keyedRDD()
    val rddRight = other.keyedRDD()

    val joinedRDD = joinType match {
      case "left" => rddLeft.leftOuterJoin(rddRight).map { case (k, (l, r)) => merger(k, l, r.orNull) }
      case "right" => rddLeft.rightOuterJoin(rddRight).map { case (k, (l, r)) => merger(k, l.orNull, r) }
      case "inner" => rddLeft.join(rddRight).map { case (k, (l, r)) => merger(k, l, r) }
      case "outer" => rddLeft.fullOuterJoin(rddRight).map { case (k, (l, r)) => merger(k, l.orNull, r.orNull) }
      case _ => fatal("Invalid join type specified. Choose one of `left', `right', `inner', `outer'")
    }

    copy(rdd = joinedRDD, signature = newSignature, key = key)
  }

  def forall(code: String): Boolean = {
    val ec = rowEvalContext()

    val f: () => java.lang.Boolean = Parser.parseTypedExpr[java.lang.Boolean](code, ec)(boxedboolHr)

    rdd.forall { a =>
      ec.setAllFromRow(a)
      val b = f()
      if (b == null)
        false
      else
        b
    }
  }

  def exists(code: String): Boolean = {
    val ec = rowEvalContext()
    val f: () => java.lang.Boolean = Parser.parseTypedExpr[java.lang.Boolean](code, ec)(boxedboolHr)

    rdd.exists { r =>
      ec.setAllFromRow(r)
      val b = f()
      if (b == null)
        false
      else
        b
    }
  }

  def export(output: String, typesFile: String = null, header: Boolean = true, exportType: Int = ExportType.CONCATENATED) {
    val hConf = hc.hadoopConf
    hConf.delete(output, recursive = true)

    Option(typesFile).foreach { file =>
      exportTypes(file, hConf, fields.map(f => (f.name, f.typ)))
    }

    val localTypes = fields.map(_.typ)

    rdd.mapPartitions { it =>
      val sb = new StringBuilder()

      it.map { r =>
        sb.clear()

        localTypes.indices.foreachBetween { i =>
          sb.append(TableAnnotationImpex.exportAnnotation(r.get(i), localTypes(i)))
        }(sb += '\t')

        sb.result()
      }
    }.writeTable(output, hc.tmpDir, Some(fields.map(_.name).mkString("\t")).filter(_ => header), exportType = exportType)
  }

  def jToMatrixTable(rowKeys: java.util.ArrayList[String],
    colKeys: java.util.ArrayList[String],
    rowFields: java.util.ArrayList[String],
    colFields: java.util.ArrayList[String],
    partitionKeys: java.util.ArrayList[String],
    nPartitions: java.lang.Integer): MatrixTable = {

    toMatrixTable(rowKeys.asScala.toArray, colKeys.asScala.toArray,
      rowFields.asScala.toArray, colFields.asScala.toArray,
      partitionKeys.asScala.toArray,
      Option(nPartitions)
    )
  }

  def toMatrixTable(rowKeys: Array[String], colKeys: Array[String], rowFields: Array[String], colFields: Array[String],
    partitionKeys: Array[String], nPartitions: Option[Int] = None): MatrixTable = {

    // all keys accounted for
    assert(rowKeys.length + colKeys.length == key.length)
    assert(rowKeys.toSet.union(colKeys.toSet) == key.toSet)

    // no fields used twice
    val fieldsUsed = mutable.Set.empty[String]
    (rowKeys ++ colKeys ++ rowFields ++ colFields).foreach { f =>
      assert(!fieldsUsed.contains(f))
      fieldsUsed += f
    }

    val entryFields = fieldNames.filter(f => !fieldsUsed.contains(f))

    // need keys for rows and cols
    assert(rowKeys.nonEmpty)
    assert(colKeys.nonEmpty)

    // check partition key is appropriate and not empty
    assert(rowKeys.startsWith(partitionKeys))
    assert(partitionKeys.nonEmpty)

    val localRVType = signature

    val colKeyIndices = colKeys.map(signature.fieldIdx(_))
    val colValueIndices = colFields.map(signature.fieldIdx(_))

    val localColData = rvd.mapPartitions { it =>
      val ur = new UnsafeRow(localRVType)
      it.map { rv =>
        val rvCopy = rv.copy()
        ur.set(rvCopy)

        val colKey = Row.fromSeq(colKeyIndices.map(ur.get))
        val colValues = Row.fromSeq(colValueIndices.map(ur.get))
        colKey -> colValues
      }
    }.reduceByKey({ case (l, _) => l }) // poor man's distinctByKey
      .collect()

    val nCols = localColData.length
    info(s"found $nCols columns")

    val colIndexBc = hc.sc.broadcast(localColData.zipWithIndex
      .map { case ((k, _), i) => (k, i) }
      .toMap)

    val rowType = TStruct((rowKeys ++ rowFields).map(f => f -> signature.fieldByName(f).typ): _*)
    val colType = TStruct((colKeys ++ colFields).map(f => f -> signature.fieldByName(f).typ): _*)
    val entryType = TStruct(entryFields.map(f => f -> signature.fieldByName(f).typ): _*)

    val colDataConcat = localColData.map { case (keys, values) => Row.fromSeq(keys.toSeq ++ values.toSeq): Annotation }

    // allFieldIndices has all row + entry fields
    val allFieldIndices = rowKeys.map(signature.fieldIdx(_)) ++ rowFields.map(signature.fieldIdx(_)) ++ entryFields.map(signature.fieldIdx(_))

    // FIXME replace with field namespaces
    val INDEX_UID = "*** COL IDX ***"

    // row and entry fields, plus an integer index
    val rowEntryStruct = rowType ++ entryType ++ TStruct(INDEX_UID -> TInt32Optional)

    val rowEntryRVD = rvd.mapPartitions(rowEntryStruct) { it =>
      val ur = new UnsafeRow(localRVType)
      val rvb = new RegionValueBuilder()
      val rv2 = RegionValue()

      it.map { rv =>
        rvb.set(rv.region)

        rvb.start(rowEntryStruct)
        rvb.startStruct()

        // add all non-col fields
        var i = 0
        while (i < allFieldIndices.length) {
          rvb.addField(localRVType, rv, allFieldIndices(i))
          i += 1
        }

        // look up col key, replace with int index
        ur.set(rv)
        val colKey = Row.fromSeq(colKeyIndices.map(ur.get))
        val idx = colIndexBc.value(colKey)
        rvb.addInt(idx)

        rvb.endStruct()
        rv2.set(rv.region, rvb.end())
        rv2
      }
    }

    val ordType = new OrderedRVType(partitionKeys, rowKeys ++ Array(INDEX_UID), rowEntryStruct)
    val ordered = OrderedRVD(ordType, rowEntryRVD.rdd, None, None)

    val matrixType: MatrixType = MatrixType.fromParts(globalSignature,
      colKeys,
      colType,
      partitionKeys,
      rowKeys,
      rowType,
      entryType)

    val orderedEntryIndices = entryFields.map(rowEntryStruct.fieldIdx)
    val orderedRKIndices = rowKeys.map(rowEntryStruct.fieldIdx)
    val orderedRowIndices = (rowKeys ++ rowFields).map(rowEntryStruct.fieldIdx)

    val idxIndex = rowEntryStruct.fieldIdx(INDEX_UID)
    assert(idxIndex == rowEntryStruct.size - 1)

    val newRVType = matrixType.rvRowType
    val orderedRKStruct = matrixType.rowKeyStruct

    val newRVD = ordered.mapPartitionsPreservesPartitioning(matrixType.orderedRVType) { it =>
      new Iterator[RegionValue] {
        val region = Region()
        val rvb = new RegionValueBuilder(region)
        val rv = RegionValue(region)
        var rvRowKey: WritableRegionValue = WritableRegionValue(orderedRKStruct)
        var currentRowKey: WritableRegionValue = WritableRegionValue(orderedRKStruct)
        var current: RegionValue = null
        var isEnd: Boolean = false

        def hasNext: Boolean = {
          if (isEnd || (current == null && !it.hasNext)) {
            isEnd = true
            return false
          }
          if (current == null)
            current = it.next()

          currentRowKey.setSelect(rowEntryStruct, orderedRKIndices, current)
          true
        }

        def next(): RegionValue = {
          if (!hasNext)
            throw new java.util.NoSuchElementException()
          region.clear()
          rvb.start(newRVType)
          rvb.startStruct()
          var i = 0
          while (i < orderedRowIndices.length) {
            rvb.addField(rowEntryStruct, current, orderedRowIndices(i))
            i += 1
          }

          rvRowKey.setSelect(rowEntryStruct, orderedRKIndices, current)

          rvb.startArray(nCols)
          i = 0

          // hasNext updates current
          while (i < nCols && hasNext && orderedRKStruct.unsafeOrdering().compare(rvRowKey.value, currentRowKey.value) == 0) {
            val nextInt = current.region.loadInt(rowEntryStruct.fieldOffset(current.offset, idxIndex))
            while (i < nextInt) {
              rvb.setMissing()
              i += 1
            }

            rvb.startStruct()
            var j = 0
            while (j < orderedEntryIndices.length) {
              rvb.addField(rowEntryStruct, current, orderedEntryIndices(j))
              j += 1
            }
            rvb.endStruct()
            current = null
            i += 1
          }
          while (i < nCols) {
            rvb.setMissing()
            i += 1
          }
          rvb.endArray()
          rvb.endStruct()
          rv.setOffset(rvb.end())
          rv
        }
      }
    }
    new MatrixTable(hc, matrixType,
      MatrixLocalValue(globals: Annotation, colDataConcat: IndexedSeq[Annotation]),
      newRVD)
  }

  def aggregate(keyCond: String, aggCond: String, nPartitions: Option[Int] = None): Table = {

    val aggregationST = (fields.map { f => f.name -> f.typ } ++
      globalSignature.fields.map { f => f.name -> f.typ })
      .zipWithIndex
      .map { case ((name, t), i) => name -> (i, t) }
      .toMap

    val ec = EvalContext((fields.map { f => f.name -> TAggregable(f.typ, aggregationST) } ++
      globalSignature.fields.map { f => f.name -> f.typ })
      .zipWithIndex
      .map { case ((name, t), i) => name -> (i, t) }
      .toMap)

    val keyEC = EvalContext(aggregationST)

    val g = globals.asInstanceOf[Row]
    var i = 0
    while (i < globalSignature.size) {
      ec.set(nColumns + i, g.get(i))
      keyEC.set(nColumns + i, g.get(i))
      i += 1
    }

    val (keyPaths, keyTypes, keyF) = Parser.parseAnnotationExprs(keyCond, keyEC, None)

    val (aggPaths, aggTypes, aggF) = Parser.parseAnnotationExprs(aggCond, ec, None)

    val newKey = keyPaths.map(_.head)
    val aggNames = aggPaths.map(_.head)

    val keySignature = TStruct((newKey, keyTypes).zipped.toSeq: _*)
    val aggSignature = TStruct((aggNames, aggTypes).zipped.toSeq: _*)

    val (zVals, seqOp, combOp, resultOp) = Aggregators.makeFunctions[Row](ec, {
      case (ec, r) =>
        ec.setAllFromRow(r)
    })

    val newRDD = rdd.mapPartitions {
      it =>
        it.map {
          r =>
            keyEC.setAllFromRow(r)
            val key = Row.fromSeq(keyF())
            (key, r)
        }
    }.aggregateByKey(zVals, nPartitions.getOrElse(this.nPartitions))(seqOp, combOp)
      .map {
        case (k, agg) =>
          resultOp(agg)
          Row.fromSeq(k.toSeq ++ aggF())
      }

    copy(rdd = newRDD, signature = keySignature.merge(aggSignature)._1, key = newKey)
  }

  def ungroup(column: String, mangle: Boolean = false): Table = {
    val (finalSignature, ungrouper) = signature.ungroup(column, mangle)
    copy(rdd = rdd.map(ungrouper), signature = finalSignature)
  }

  def group(dest: String, columns: java.util.ArrayList[String]): Table = group(dest, columns.asScala.toArray)

  def group(dest: String, columns: Array[String]): Table = {
    val (newSignature, grouper) = signature.group(dest, columns)
    val newKey = key.filter(!columns.contains(_))
    copy(rdd = rdd.map(grouper), signature = newSignature, key = newKey)
  }

  def expandTypes(): Table = {
    val localSignature = signature
    val expandedSignature = Annotation.expandType(localSignature).asInstanceOf[TStruct]

    copy(rdd = rdd.map { a => Annotation.expandAnnotation(a, localSignature).asInstanceOf[Row] },
      signature = expandedSignature,
      key = key)
  }

  def flatten(): Table = {
    val localSignature = signature
    val keySignature = TStruct(keyFields.map { f => f.name -> f.typ }: _*)
    val flattenedSignature = Annotation.flattenType(localSignature).asInstanceOf[TStruct]
    val flattenedKey = Annotation.flattenType(keySignature).asInstanceOf[TStruct].fields.map(_.name).toArray

    copy(rdd = rdd.map { a => Annotation.flattenAnnotation(a, localSignature).asInstanceOf[Row] },
      signature = flattenedSignature,
      key = flattenedKey)
  }

  def toDF(sqlContext: SQLContext): DataFrame = {
    val localSignature = signature
    sqlContext.createDataFrame(
      rdd.map {
        a => SparkAnnotationImpex.exportAnnotation(a, localSignature).asInstanceOf[Row]
      },
      signature.schema.asInstanceOf[StructType])
  }

  def explode(columnToExplode: String): Table = {

    val explodeField = signature.fieldOption(columnToExplode) match {
      case Some(x) => x
      case None =>
        fatal(
          s"""Input field name `${ columnToExplode }' not found in Table.
             |Table field names are `${ fieldNames.mkString(", ") }'.""".stripMargin)
    }

    val index = explodeField.index

    val explodeType = explodeField.typ match {
      case t: TIterable => t.elementType
      case _ => fatal(s"Require Array or Set. Column `$columnToExplode' has type `${ explodeField.typ }'.")
    }

    val newSignature = signature.copy(fields = fields.updated(index, Field(columnToExplode, explodeType, index)))

    val empty = Iterable.empty[Row]
    val explodedRDD = rdd.flatMap { a =>
      val row = a.toSeq
      val it = row(index)
      if (it == null)
        empty
      else
        for (element <- row(index).asInstanceOf[Iterable[_]]) yield Row.fromSeq(row.updated(index, element))
    }

    copy(rdd = explodedRDD, signature = newSignature, key = key)
  }

  def explode(columnNames: Array[String]): Table = {
    columnNames.foldLeft(this)((kt, name) => kt.explode(name))
  }

  def explode(columnNames: java.util.ArrayList[String]): Table = explode(columnNames.asScala.toArray)

  def collect(): IndexedSeq[Row] = rdd.collect()

  def write(path: String, overwrite: Boolean = false) {
    if (!path.endsWith(".kt") && !path.endsWith(".kt/"))
      fatal(s"write path must end in '.kt', but found '$path'")

    if (overwrite)
      hc.hadoopConf.delete(path, recursive = true)
    else if (hc.hadoopConf.exists(path))
      fatal(s"$path already exists")

    hc.hadoopConf.mkDir(path)

    val partitionCounts = rvd.rdd.writeRows(path, signature)

    val refPath = path + "/references/"
    hc.hadoopConf.mkDir(refPath)
    GenomeReference.exportReferences(hc, refPath, signature)
    GenomeReference.exportReferences(hc, refPath, globalSignature)

    val metadata = TableMetadata(Table.fileVersion,
      hc.version,
      key,
      ir.typ.toString,
      JSONAnnotationImpex.exportAnnotation(globals, globalSignature),
      nPartitions,
      partitionCounts)

    hc.hadoopConf.writeTextFile(path + "/metadata.json.gz")(out =>
      Serialization.write(metadata, out))

    hc.hadoopConf.writeTextFile(path + "/_SUCCESS")(out => ())
  }

  def cache(): Table = persist("MEMORY_ONLY")

  def persist(storageLevel: String): Table = {
    val level = try {
      StorageLevel.fromString(storageLevel)
    } catch {
      case e: IllegalArgumentException =>
        fatal(s"unknown StorageLevel `$storageLevel'")
    }

    rdd.persist(level)
    this
  }

  def unpersist() {
    rdd.unpersist()
  }

  def orderBy(sortCols: SortColumn*): Table =
    orderBy(sortCols.toArray)

  def orderBy(sortCols: Array[SortColumn]): Table = {
    val sortColIndexOrd = sortCols.map { case SortColumn(n, so) =>
      val i = signature.fieldIdx(n)
      val f = signature.fields(i)
      val fo = f.typ.ordering
      (i, if (so == Ascending) fo else fo.reverse)
    }

    val ord: Ordering[Annotation] = new Ordering[Annotation] {
      def compare(a: Annotation, b: Annotation): Int = {
        var i = 0
        while (i < sortColIndexOrd.length) {
          val (fi, ford) = sortColIndexOrd(i)
          val c = ford.compare(
            a.asInstanceOf[Row].get(fi),
            b.asInstanceOf[Row].get(fi))
          if (c != 0) return c
          i += 1
        }

        0
      }
    }

    val act = implicitly[ClassTag[Annotation]]
    copy(rdd = rdd.sortBy(identity[Annotation], ascending = true)(ord, act))
  }

  def exportSolr(zkHost: String, collection: String, blockSize: Int = 100): Unit = {
    SolrConnector.export(this, zkHost, collection, blockSize)
  }

  def exportCassandra(address: String, keyspace: String, table: String,
    blockSize: Int = 100, rate: Int = 1000): Unit = {
    CassandraConnector.export(this, address, keyspace, table, blockSize, rate)
  }

  def repartition(n: Int, shuffle: Boolean = true): Table = copy(rdd = rdd.coalesce(n, shuffle))

  def union(kts: java.util.ArrayList[Table]): Table = union(kts.asScala.toArray: _*)

  def union(kts: Table*): Table = {
    kts.foreach { kt =>
      if (signature != kt.signature)
        fatal("cannot union tables with different schemas")
      if (!key.sameElements(kt.key))
        fatal("cannot union tables with different key")
    }

    copy(rdd = hc.sc.union(rdd, kts.map(_.rdd): _*))
  }

  def take(n: Int): Array[Row] = rdd.take(n)

  def sample(p: Double, seed: Int = 1): Table = {
    require(p > 0 && p < 1, s"the 'p' parameter must fall between 0 and 1, found $p")
    copy2(rvd = rvd.sample(withReplacement = false, p, seed))
  }

  def index(name: String = "index"): Table = {
    if (fieldNames.contains(name))
      fatal(s"name collision: cannot index table, because column '$name' already exists")

    val (newSignature, ins) = signature.insert(TInt64(), name)

    val newRDD = rdd.zipWithIndex().map { case (r, ind) => ins(r, ind).asInstanceOf[Row] }

    copy(signature = newSignature.asInstanceOf[TStruct], rdd = newRDD)
  }

  def maximalIndependentSet(iExpr: String, jExpr: String, tieBreakerExpr: Option[String] = None): Array[Any] = {
    val ec = rowEvalContext()

    val (iType, iThunk) = Parser.parseExpr(iExpr, ec)
    val (jType, jThunk) = Parser.parseExpr(jExpr, ec)

    if (iType != jType)
      fatal(s"node expressions must have the same type: type of `i' is $iType, but type of `j' is $jType")

    val tieBreakerEc = EvalContext("l" -> iType, "r" -> iType)

    val maybeTieBreaker = tieBreakerExpr.map { e =>
      val tieBreakerThunk = Parser.parseTypedExpr[Int](e, tieBreakerEc)

      (l: Any, r: Any) => {
        tieBreakerEc.setAll(l, r)
        tieBreakerThunk()
      }
    }.getOrElse(null)

    val edgeRdd = mapAnnotations { r =>
      ec.setAllFromRow(r)
      (iThunk(), jThunk())
    }

    if (edgeRdd.count() > 400000)
      warn(s"over 400,000 edges are in the graph; maximal_independent_set may run out of memory")

    Graph.maximalIndependentSet(edgeRdd.collect(), maybeTieBreaker)
  }

  def show(n: Int = 10, truncate: Option[Int] = None, printTypes: Boolean = true, maxWidth: Int = 100): Unit = {
    println(showString(n, truncate, printTypes, maxWidth))
  }

  def showString(n: Int = 10, truncate: Option[Int] = None, printTypes: Boolean = true, maxWidth: Int = 100): String = {
    /**
      * Parts of this method are lifted from:
      *   org.apache.spark.sql.Dataset.showString
      * Spark version 2.0.2
      */

    truncate.foreach { tr => require(tr > 3, s"truncation length too small: $tr") }
    require(maxWidth >= 10, s"max width too small: $maxWidth")

    val (data, hasMoreData) = if (n < 0)
      collect() -> false
    else {
      val takeResult = take(n + 1)
      val hasMoreData = takeResult.length > n
      (takeResult.take(n): IndexedSeq[Row]) -> hasMoreData
    }

    def convertType(t: Type, name: String, ab: ArrayBuilder[(String, String, Boolean)]) {
      t match {
        case s: TStruct => s.fields.foreach { f =>
          convertType(f.typ, if (name == null) f.name else name + "." + f.name, ab)
        }
        case _ =>
          ab += (name, t.toString, t.isInstanceOf[TNumeric])
      }
    }

    val headerBuilder = new ArrayBuilder[(String, String, Boolean)]()
    convertType(signature, null, headerBuilder)
    val (names, types, rightAlign) = headerBuilder.result().unzip3

    def convertValue(t: Type, v: Annotation, ab: ArrayBuilder[String]) {
      t match {
        case s: TStruct =>
          val r = v.asInstanceOf[Row]
          s.fields.foreach(f => convertValue(f.typ, if (r == null) null else r.get(f.index), ab))
        case _ =>
          ab += t.str(v)
      }
    }

    val valueBuilder = new ArrayBuilder[String]()
    val dataStrings = data.map { r =>
      valueBuilder.clear()
      convertValue(signature, r, valueBuilder)
      valueBuilder.result()
    }

    val fixedWidth = 4 // "| " + " |"
    val delimWidth = 3 // " | "

    val tr = truncate.getOrElse(maxWidth - 4)

    val allStrings = (Iterator(names, types) ++ dataStrings.iterator).map { arr =>
      arr.map { str => if (str.length > tr) str.substring(0, tr - 3) + "..." else str }
    }.toArray

    val nCols = names.length
    val colWidths = Array.fill(nCols)(0)

    // Compute the width of each column
    for (i <- allStrings.indices)
      for (j <- 0 until nCols)
        colWidths(j) = math.max(colWidths(j), allStrings(i)(j).length)

    val normedStrings = allStrings.map { line =>
      line.zipWithIndex.map { case (cell, i) =>
        if (rightAlign(i))
          StringUtils.leftPad(cell, colWidths(i))
        else
          StringUtils.rightPad(cell, colWidths(i))
      }
    }

    val sb = new StringBuilder()
    sb.clear()

    // writes cols [startIndex, endIndex)
    def writeCols(startIndex: Int, endIndex: Int) {

      val toWrite = (startIndex until endIndex).toArray

      val sep = toWrite.map(i => "-" * colWidths(i)).addString(new StringBuilder, "+-", "-+-", "-+\n").result()
      // add separator line
      sb.append(sep)

      // add column names
      toWrite.map(normedStrings(0)(_)).addString(sb, "| ", " | ", " |\n")

      // add separator line
      sb.append(sep)

      if (printTypes) {
        // add types
        toWrite.map(normedStrings(1)(_)).addString(sb, "| ", " | ", " |\n")

        // add separator line
        sb.append(sep)
      }

      // data
      normedStrings.drop(2).foreach {
        toWrite.map(_).addString(sb, "| ", " | ", " |\n")
      }

      // add separator line
      sb.append(sep)
    }

    if (nCols == 0)
      writeCols(0, 0)

    var colIdx = 0
    var first = true

    while (colIdx < nCols) {
      val startIdx = colIdx
      var colWidth = fixedWidth

      // consume at least one column, and take until the next column would put the width over maxWidth
      do {
        colWidth += 3 + colWidths(colIdx)
        colIdx += 1
      } while (colIdx < nCols && colWidth + delimWidth + colWidths(colIdx) <= maxWidth)

      if (!first) {
        sb.append('\n')
      }

      writeCols(startIdx, colIdx)

      first = false
    }

    if (hasMoreData)
      sb.append(s"showing top $n ${ plural(n, "row") }\n")

    sb.result()
  }

  def copy(rdd: RDD[Row] = rdd,
    signature: TStruct = signature,
    key: Array[String] = key,
    globalSignature: TStruct = globalSignature,
    globals: Row = globals): Table = {
    Table(hc, rdd, signature, key, globalSignature, globals)
  }

  def copy2(rvd: RVD = rvd,
    signature: TStruct = signature,
    key: Array[String] = key,
    globalSignature: TStruct = globalSignature,
    globals: Row = globals): Table = {
    new Table(hc, TableLiteral(
      TableValue(TableType(signature, key, globalSignature), TableLocalValue(globals), rvd)
    ))
  }

  def toOrderedRVD(hintPartitioner: Some[OrderedRVPartitioner], partitionKeys: Int): OrderedRVD = {
    val localSignature = signature

    val orderedKTType = new OrderedRVType(key.take(partitionKeys), key, signature)
    assert(hintPartitioner.forall(p => p.pkType.fieldType.sameElements(orderedKTType.pkType.fieldType)))
    OrderedRVD(orderedKTType, rvd.rdd, None, hintPartitioner)
  }
}
