package is.hail.seqr

import is.hail.HailContext
import is.hail.annotations.Annotation
import is.hail.io.{CassandraConnector, CassandraImpex}
import is.hail.keytable.KeyTable
import org.apache.solr.client.solrj.{SolrClient, SolrQuery}
import is.hail.utils._
import is.hail.variant.{Variant, VariantDataset, VariantIndexedVDS}
import org.apache.solr.common.SolrDocument
import org.http4s.headers.`Content-Type`
import org.http4s._
import org.http4s.MediaType._
import org.http4s.dsl._
import org.http4s.server._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

case class SeqrRequest(page: Int,
  limit: Int,
  sort_by: Option[JObject],
  sample_ids: Option[Array[String]],
  variant_filters: Option[Map[String, JObject]],
  genotype_filters: Option[Map[String, Map[String, JObject]]]) {

  def emitExprValue(jv: JValue): String = (jv: @unchecked) match {
    case JInt(i) => i.toString
    case JString(s) => '"' + s + '"'
    case JBool(b) => if (b) "true" else "false"
    case JDouble(d) => d.toString
  }

  def emitExprPredicate(field: String, pred: JObject): String = pred match {
    case JObject(List(("eq", v))) =>
      s"($field == ${ v.values })"
    case JObject(List(("in", JArray(elems)))) =>
      s"(${
        elems.map { elem =>
          s"($field == ${ emitExprValue(elem) })"
        }.mkString(" || ")
      })"
    case JObject(List(("range", JArray(List(from, to))))) =>
      s"((${
        if (from == JString("*"))
          "true"
        else
          s"$field >= ${ emitExprValue(from) }"
      }) && (${
        if (to == JString("*"))
          "true"
        else
          s"$field <= ${ emitExprValue(to) }"
      }))"
  }

  def emitExprQuery(): String = {
    s"(${
      val p = Array("true") ++
        variant_filters.getOrElse(Map.empty)
          .map { case (f, p) => emitExprPredicate(s"${ q(f) }", p) } ++
        genotype_filters.getOrElse(Map.empty)
          .flatMap { case (s, sfilters) =>
            sfilters.map { case (f, p) =>
              emitExprPredicate(s"${ q(s) }__${ q(f) }", p)
            }
          }
      s"(${ p.mkString(" && ") })"
    })"
  }

  def filterKT(kt: KeyTable): KeyTable = {
    val q = emitExprQuery()
    println(s"q = $q")
    kt.filter(q, keep = true)
  }

  def sampleFilters(): Option[Set[String]] = sample_ids.map(_.toSet)
}

object SeqrRequest {
  def matchesRef(predicate: JValue): Boolean = (predicate: @unchecked) match {
    case JObject(List(("eq", JInt(x)))) =>
      x == 0
    case JObject(List(("in", JArray(xs)))) =>
      xs.exists(jv => (jv: @unchecked) match {
        case JInt(x) => x == 0
      })
    case JObject(List(("range", JArray(List(low, high))))) =>
      (low == JString("*") ||
        low.asInstanceOf[JInt].num <= 0) &&
        (high == JString("*")
          || high.asInstanceOf[JInt].num >= 0)
  }
}

case class SeqrResponse(is_error: Boolean,
  error_message: Option[String],
  variants: Option[Array[JObject]])

trait SeqrHandler {
  def process(req: SeqrRequest): SeqrResponse
}

trait SearchEngine {
  def search(req: SeqrRequest): Seq[Key]
}

trait LookupEngine {
  def lookup(req: SeqrRequest, keys: Seq[Key]): SeqrResponse
}

class CompoundSeqrHandler(searchEngine: SearchEngine, queryEngine: LookupEngine) extends SeqrHandler {
  def process(req: SeqrRequest): SeqrResponse =
    queryEngine.lookup(req, searchEngine.search(req))
}

case class Key(dataset: String,
  chrom: String, start: Int, ref: String, alt: String)

class SolrEngine(solr: SolrClient) {
  def emitFilter(field: String, pred: JValue): String = {
    // println("pred", pred)
    pred match {
      case JObject(List(("eq", value))) =>
        s"$field:${ value.values }"

      case JObject(List(("range", JArray(List(from, to))))) =>
        s"$field:[${ from.values } TO ${ to.values }]"

      case JObject(List(("in", JArray(elems)))) =>
        var t = elems
          .map(_.values)
          .mkString(" OR ")
        t = s"$field:( $t )"
        t
    }
  }

  def emitVariantFilters(variantFilters: Map[String, JObject]): Seq[String] = {
    variantFilters.map { case (f, pred) => emitFilter(f, pred) }.toSeq
  }

  def emitGenotypeFilters(genotypeFilters: Map[String, Map[String, JObject]]): Seq[String] = {
    genotypeFilters.flatMap { case (s, gpred) =>
      gpred.map { case (f, pred) =>
        emitFilter(s"${ q(s) }__${ q(f) }", pred)
      }
    }.toSeq
  }

  def emitQuery(req: SeqrRequest): String = {
    val filters = req.variant_filters.map(emitVariantFilters).getOrElse(Seq.empty) ++
      req.genotype_filters.map(emitGenotypeFilters).getOrElse(Seq.empty)

    if (filters.isEmpty)
      "*:*"
    else
      filters.mkString(" AND ")
  }

  def buildQuery(req: SeqrRequest): SolrQuery = {
    val qt = emitQuery(req)
    println(s"qt: $qt")

    var query = new SolrQuery

    query.setQuery(qt)

    req.sort_by.foreach(_.obj.foreach {
      case (f, JString("asc")) =>
        query.addSort(f, SolrQuery.ORDER.asc)
      case (f, JString("desc")) =>
        query.addSort(f, SolrQuery.ORDER.desc)
    })

    query.setStart((req.page - 1) * req.limit)
    query.setRows(req.limit)

    query
  }

  def solrQuery(req: SeqrRequest): (Long, Seq[SolrDocument]) = {
    val query = buildQuery(req)

    val solrResponse = solr.query(query)
    // println(solrResponse)

    val docs = solrResponse.getResults
    val found = docs.getNumFound

    (found, docs.asScala)
  }

  def docToJSON(doc: SolrDocument, sampleFilters: Option[Set[String]]): JObject = {
    val variants = mutable.Map.empty[String, JValue]
    val genotypes = mutable.Map.empty[String, mutable.Map[String, JValue]]

    doc.keySet.asScala
      .filter(_ != "_version_")
      .foreach { name =>

        def toJSON(v: Any): JValue = v match {
          case null => JNull
          case b: java.lang.Boolean => JBool(b)
          case i: Int => JInt(i)
          case i: Long => JInt(i)
          case d: Double => JDouble(d)
          case f: Float => JDouble(f)
          case al: java.util.ArrayList[_] =>
            JArray(al.asScala.map(vi => toJSON(vi)).toList)
          case s: String => JString(s)
        }

        var jv = toJSON(doc.getFieldValue(name))
        unescapeColumnName(name) match {
          case Left(vfield) =>
            variants += vfield -> jv

          case Right((sample, gfield)) =>
            val include = sampleFilters.forall(_.contains(sample))
            if (include) {
              genotypes.updateValue(sample, mutable.Map.empty, { m =>
                m += ((gfield, jv))
                m
              })
            }
        }
      }

    if (genotypes.nonEmpty)
      variants += "genotypes" -> JObject(genotypes.map { case (k, v) =>
        (k, JObject(v.toList))
      }.toList)

    JObject(variants.toList)
  }
}

class SolrHandler(solr: SolrClient) extends SolrEngine(solr) with SeqrHandler {
  def process(req: SeqrRequest): SeqrResponse = {
    val (found, docs) = solrQuery(req)

    val sampleFilters = req.sample_ids.map(_.toSet)

    SeqrResponse(is_error = false,
      None,
      Some(docs.map { doc => docToJSON(doc, sampleFilters) }.toArray))
  }
}

class SolrSearchEngine(solr: SolrClient) extends SolrEngine(solr) with SearchEngine {
  override def emitGenotypeFilters(genotypeFilters: Map[String, Map[String, JObject]]): Seq[String] = {
    genotypeFilters.flatMap { case (s, spreds) =>
      val fs = spreds.map { case (f, pred) =>
        emitFilter(s"${ q(s) }__${ q(f) }", pred)
      }

      if (spreds.exists { case (s, pred) =>
        s == "num_alt" && SeqrRequest.matchesRef(pred)
      }) {
        var t = fs.mkString(" AND ")
        t = s"-${ q(s) }__${ q("num_alt") }:*"
        Seq(t)
      } else
        fs
    }.toSeq
  }

  override def buildQuery(req: SeqrRequest): SolrQuery = {
    val query = super.buildQuery(req)
    query.addField("dataset_5fid")
    query.addField("chrom")
    query.addField("start")
    query.addField("ref")
    query.addField("alt")
    query
  }

  def search(req: SeqrRequest): Seq[Key] = {
    val (found, docs) = solrQuery(req)
    docs.map { doc =>
      Key(doc.getFieldValue("dataset_5fid").asInstanceOf[String],
        doc.getFieldValue("chrom").asInstanceOf[String],
        doc.getFieldValue("start").asInstanceOf[Int],
        doc.getFieldValue("ref").asInstanceOf[String],
        doc.getFieldValue("alt").asInstanceOf[String])
    }
  }
}

class CassLookupEngine(hc: HailContext, address: String, keyspace: String, table: String) extends LookupEngine {
  private val session = CassandraConnector.getSession(address)

  private val keyspaceMetadata = session.getCluster.getMetadata.getKeyspace(keyspace)
  private val tableMetadata = keyspaceMetadata.getTable(table)

  private val tableType = CassandraImpex.importSchema(tableMetadata)

  def lookup(req: SeqrRequest, keys: Seq[Key]): SeqrResponse = {
    val sampleFilters = req.sample_ids.map(_.toSet)

    val localAddress = address
    val localKeyspace = keyspace
    val localTable = table

    val rows = hc.sc.parallelize(keys)
      .mapPartitions { it =>
        val session = CassandraConnector.getSession(localAddress)

        val prepared = session.prepare(
          s"SELECT * FROM ${ localKeyspace }.${ localTable } WHERE dataset_5fid=? AND chrom=? AND start=? AND ref=? AND alt=?")

        val keyspaceMetadata = session.getCluster.getMetadata.getKeyspace(localKeyspace)
        val tableMetadata = keyspaceMetadata.getTable(localTable)

        it.map { key =>
          session.executeAsync(prepared.bind(key.dataset, key.chrom, key.start: java.lang.Integer, key.ref, key.alt))
        }.map { future =>
          val results = future.getUninterruptibly()
          val row = results.one()
          CassandraImpex.importRow(tableMetadata, row): Annotation
        }
      }

    ktToResponse(req, KeyTable(hc, rows, tableType))
  }
}

class VDSHandler(hc: HailContext,
  variantExpr: String, genotypeExpr: String, datasetPath: String) extends SeqrHandler {

  val vds: VariantDataset = hc.read(datasetPath)
  val kt: KeyTable = vds.renameSamples(vds.sampleIds.map(s => s -> q(s)).toMap)
    .makeKT(variantExpr, genotypeExpr, Array.empty, seperator = "__")

  def process(req: SeqrRequest): SeqrResponse = {
    val kt2 = req.filterKT(kt)

    val limit = req.limit
    val page = req.page
    assert(limit >= 0 && page >= 1)

    val toTake = page * limit
    val toDrop = (page - 1) * limit

    val variants = kt2.rdd
      .take(toTake)
      .drop(toDrop)
      .map(r => rowToVariant(req.sampleFilters(), r, kt2.signature))

    SeqrResponse(is_error = false, None, Some(variants))
  }
}

class VDSLookupEngine(
  hc: HailContext, vExpr: String, gExpr: String, datasetPath: String) extends LookupEngine {

  private val vivds = VariantIndexedVDS(hc, datasetPath)
  private val metadata = vivds.metadata

  def lookup(req: SeqrRequest, keys: Seq[Key]): SeqrResponse = {
    val vds = vivds.query(
      keys.map(k =>
        Variant(k.chrom, k.start, k.ref, k.alt)).toArray)
    val kt = vds
      .renameSamples(vds.sampleIds.map(s => s -> q(s)).toMap)
      .makeKT(vExpr, gExpr, seperator = "__")

    ktToResponse(req, kt)
  }
}

class SeqrService(handler: SeqrHandler) {
  def service(implicit executionContext: ExecutionContext = ExecutionContext.global): HttpService = Router(
    "" -> rootService)

  def rootService(implicit executionContext: ExecutionContext) = HttpService {
    case req@POST -> Root =>

      req.decode[String] { text =>
        // println(text)

        try {
          // FIXME should be automatic
          val knownKeys = Set("page",
            "limit",
            "sort_by",
            "sample_ids",
            "variant_filters",
            "genotype_filters")

          val json = parse(text)
          (json: @unchecked) match {
            case JObject(fields) =>
              fields.foreach { case (id, _) =>
                if (!knownKeys.contains(id))
                  throw new IllegalArgumentException(s"unknown field $id in request")
              }
          }

          val req = json.extract[SeqrRequest]
          // val req = read[SeqrRequest](text)

          val result = handler.process(req)
          Ok(write(result))
            .putHeaders(`Content-Type`(`application/json`))
        } catch {
          case e: Exception =>
            println(expandException(e, true))
            val result = SeqrResponse(is_error = true, Some(e.getMessage), None)
            BadRequest(write(result))
              .putHeaders(`Content-Type`(`application/json`))
        }
      }
  }
}