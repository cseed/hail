package is.hail

import is.hail.expr.{JSONAnnotationImpex, TString, TStruct}
import is.hail.keytable.KeyTable
import is.hail.utils.StringEscapeUtils.{escapeIdentifier, unescapeIdentifier}
import is.hail.utils._
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.spark.sql.Row
import org.http4s.server.Server
import org.http4s.server.blaze.BlazeBuilder
import org.json4s.jackson.JsonMethods
import org.json4s.{JObject, JValue}

import scala.collection.mutable

package object seqr {
  def solrClient(zkHost: String, collection: String): SolrClient = {
    val cc = new CloudSolrClient.Builder()
      .withZkHost(zkHost)
      .build()
    cc.setDefaultCollection(collection)
    cc
  }

  def solrHandler(solr: SolrClient): SolrHandler =
    new SolrHandler(solr)

  def vdsHandler(hc: HailContext, vExpr: String, gExpr: String, path: String): VDSHandler =
    new VDSHandler(hc, vExpr, gExpr, path)

  def q(name: String): String = escapeIdentifier(name)

  def unq(s: String): String = unescapeIdentifier(s)

  def unescapeColumnName(escaped: String): Either[String, (String, String)] = {
    val sep = escaped.indexOf("__")
    if (sep == -1)
      Left(unq(escaped))
    else
      Right((unq(escaped.substring(0, sep)),
        unq(escaped.substring(sep + 2))))
  }

  def rowToVariant(sampleFilters: Option[Set[String]], r: Any, t: TStruct,
    jsonColumns: Set[String] = Set.empty): JObject = {
    val variants = mutable.Map.empty[String, JValue]
    val genotypes = mutable.Map.empty[String, mutable.Map[String, JValue]]

    (r.asInstanceOf[Row].toSeq, t.fields).zipped
      .map { case (a, f) =>
        unescapeColumnName(f.name) match {
          case Left(vc) =>
            if (a != null) {
              val jv =
                if (jsonColumns.contains(vc)) {
                  assert(f.typ == TString)
                  JsonMethods.parse(a.asInstanceOf[String])
                } else
                  JSONAnnotationImpex.exportAnnotation(a, f.typ)
              variants += vc -> jv
            }
          case Right((s, gc)) =>
            // FIXME filter sample
            if (sampleFilters.forall(_.contains(s)) && a != null)
              genotypes.updateValue(s, mutable.Map.empty, { m =>
                m += gc -> JSONAnnotationImpex.exportAnnotation(a, f.typ)
                m
              })
        }
      }

    if (genotypes.nonEmpty)
      variants += "genotypes" -> JObject(genotypes.map { case (k, v) =>
        (k, JObject(v.toList))
      }.toList)

    JObject(variants.toList)
  }

  def ktToResponse(req: SeqrRequest, kt: KeyTable, jsonColumns: Set[String] = Set.empty): SeqrResponse = {
    val localSampleFilters = req.sampleFilters()
    val localKTSignature = kt.signature

    val variants = req.filterKT(kt)
      .rdd
      .map(r => rowToVariant(localSampleFilters, r, localKTSignature, jsonColumns))
      .collect()

    SeqrResponse(is_error = false, None, Some(variants))
  }

  def runServer(handler: SeqrHandler, port: Int = 6060): Server = {
    BlazeBuilder.bindHttp(port, "0.0.0.0")
      .mountService(new SeqrService(handler).service, "/")
      .run
    // task.awaitShutdown()
  }
}
