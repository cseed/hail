package org.broadinstitute.hail.driver

import java.util

import org.apache.solr.client.solrj.SolrResponse
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpSolrClient}
import org.apache.solr.client.solrj.request.schema.SchemaRequest
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.apache.solr.util.SolrCLI
import org.broadinstitute.hail.utils._
import org.broadinstitute.hail.expr._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.utils.StringEscapeUtils._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

object ExportVariantsSolr extends Command with Serializable {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-c", usage = "SolrCloud collection")
    var collection: String = _

    @Args4jOption(name = "--export-missing", usage = "export missing genotypes")
    var exportMissing = false

    @Args4jOption(name = "--export-ref", usage = "export HomRef calls")
    var exportRef = false

    @Args4jOption(required = true, name = "-v",
      usage = "comma-separated list of fields/computations to be exported")
    var variantCondition: String = _

    @Args4jOption(required = true, name = "-g",
      usage = "comma-separated list of fields/computations to be exported")
    var genotypeCondition: String = _

    @Args4jOption(name = "-u", aliases = Array("--url"),
      usage = "Solr instance (URL) to connect to")
    var url: String = _

    @Args4jOption(name = "-z", aliases = Array("--zk-host"),
      usage = "Zookeeper host string to connect to")
    var zkHost: String = _

    @Args4jOption(name = "-d", aliases = Array("--drop"),
      usage = "delete Solr collection before exporting")
    var drop: Boolean = false

    @Args4jOption(name = "-n", aliases = Array("--num-shards"),
      usage = "number of shards to use when creating collection")
    var numShards: Int = 1

    @Args4jOption(required = true, name = "-s", aliases = Array("--solr-install-dir"),
      usage = "Solr installation directory, for example: /local/software/solr-6.2.1")
    var solrInstallDir: String = _
  }

  def newOptions = new Options

  def name = "exportvariantssolr"

  def description = "Export variant information to Solr"

  def supportsMultiallelic = true

  def requiresVDS = true

  def toSolrType(t: Type): String = t match {
    case TInt => "int"
    case TLong => "long"
    case TFloat => "float"
    case TDouble => "double"
    case TBoolean => "boolean"
    case TString => "string"
    // FIXME only 1 deep
    case i: TIterable => toSolrType(i.elementType)
    // FIXME
    case _ => fatal("")
  }

  def escapeString(name: String): String =
    escapeStringSimple(name, '_', !_.isLetter, !_.isLetterOrDigit)

  def addFieldReq(preexistingFields: Set[String], name: String, spec: Map[String, AnyRef], t: Type): Option[SchemaRequest.AddField] = {
    if (preexistingFields(name))
      return None

    var m = spec

    if (!m.contains("name"))
      m += "name" -> name

    if (!m.contains("type"))
      m += "type" -> toSolrType(t)

    if (!m.contains("stored"))
      m += "stored" -> true.asInstanceOf[AnyRef]

    if (!m.contains("multiValued"))
      m += "multiValued" -> t.isInstanceOf[TIterable].asInstanceOf[AnyRef]

    val req = new SchemaRequest.AddField(m.asJava)
    Some(req)
  }

  def documentAddField(document: SolrInputDocument, name: String, t: Type, value: Any) {
    if (t.isInstanceOf[TIterable]) {
      value.asInstanceOf[Traversable[_]].foreach { xi =>
        if (xi != null)
          document.addField(name, xi)
      }
    } else if (value != null)
      document.addField(name, value)
  }

  def processResponse(action: String, res: SolrResponse) {
    val tRes = res.getResponse.asScala.map { entry =>
      (entry.getKey, entry.getValue)
    }.toMap[String, AnyRef]

    tRes.get("errors") match {
      case Some(es) =>
        val errors = es.asInstanceOf[util.ArrayList[AnyRef]].asScala
        val error = errors.head.asInstanceOf[util.Map[String, AnyRef]]
          .asScala
        val errorMessages = error("errorMessages")
          .asInstanceOf[util.ArrayList[String]]
          .asScala
        fatal(s"error in $action:\n  ${ errorMessages.map(_.trim).mkString("\n    ") }${
          if (errors.length > 1)
            s"\n  and ${ errors.length - 1 } errors"
          else
            ""
        }")

      case None =>
        if (tRes.keySet != Set("responseHeader"))
          warn(s"unknown Solr response in $action: $res")
    }
  }

  def run(state: State, options: Options): State = {
    val sc = state.vds.sparkContext
    val vds = state.vds
    val vas = vds.vaSignature
    val sas = vds.saSignature
    val gCond = options.genotypeCondition
    val vCond = options.variantCondition
    val collection = options.collection
    val exportMissing = options.exportMissing
    val exportRef = options.exportRef
    val drop = options.drop
    val numShards = options.numShards
    val solrInstallDir = options.solrInstallDir

    val vSymTab = Map(
      "v" -> (0, TVariant),
      "va" -> (1, vas))
    val vEC = EvalContext(vSymTab)
    val vA = vEC.a

    val vparsed = Parser.parseSolrNamedArgs(vCond, vEC)

    val gSymTab = Map(
      "v" -> (0, TVariant),
      "va" -> (1, vas),
      "s" -> (2, TSample),
      "sa" -> (3, sas),
      "g" -> (4, TGenotype))
    val gEC = EvalContext(gSymTab)
    val gA = gEC.a

    val gparsed = Parser.parseSolrNamedArgs(gCond, gEC)

    val url = options.url
    val zkHost = options.zkHost

    if (url == null && zkHost == null)
      fatal("one of -u or -z required")

    if (url != null && zkHost != null)
      fatal("both -u and -z given")

    if (zkHost != null && collection == null)
      fatal("-c required with -z")

    if (drop) {
      var args = Array("-name", collection, "-deleteConfig", "true", "-verbose")
      if (url != null)
        args ++= Array("-solrUrl", url)
      else
        args ++= Array("-zkHost", zkHost)

      val deleteTool = new SolrCLI.DeleteTool()
      val commandLine = SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(deleteTool.getOptions), args)

      val es = deleteTool.runTool(commandLine)
      if (es != 0)
        warn(s"delete failed with exit status: $es")
      else
        info(s"deleted collection $collection")
    }

    var createArgs = Array("-name", collection,
      "-confdir", "data_driven_schema_configs",
      "-configsetsDir", solrInstallDir + "/server/solr/configsets",
      "-verbose")

    val createTool =
      if (url != null) {
        createArgs ++= Array("-solrUrl", url)
        new SolrCLI.CreateCoreTool()
      } else {
        createArgs ++= Array("-zkHost", zkHost,
          "-shards", numShards.toString,
          "-replicationFactor", "1")
        new SolrCLI.CreateCollectionTool()
      }

    val createCommandLine = SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(createTool.getOptions), createArgs)

    val createExitStatus = createTool.runTool(createCommandLine)
    if (createExitStatus != 0)
      warn(s"create $collection failed with exit status: $createExitStatus")
    else
      info(s"created new solr collection $collection with $numShards ${ plural(numShards, "shard", "s") }")

    val solr =
      if (url != null)
        new HttpSolrClient.Builder(url + "/" + collection)
          .build()
      else {
        val cc = new CloudSolrClient.Builder()
          .withZkHost(zkHost)
          .build()
        cc.setDefaultCollection(collection)
        cc
      }

    // retrieve current fields
    val fieldsResponse = new SchemaRequest.Fields().process(solr)

    val preexistingFields = fieldsResponse.getFields.asScala
      .map(_.asScala("name").asInstanceOf[String])
      .toSet

    val addFieldReqs = vparsed.flatMap { case (name, spec, t, f) =>
      addFieldReq(preexistingFields, escapeString(name), spec, t)
    } ++ vds.sampleIds.flatMap { s =>
      gparsed.flatMap { case (name, spec, t, f) =>
        val fname = escapeString(s) + "__" + escapeString(name)
        addFieldReq(preexistingFields, fname, spec, t)
      }
    }

    info(s"adding ${
      addFieldReqs.length
    } fields")

    if (addFieldReqs.nonEmpty) {
      val req = new SchemaRequest.MultiUpdate((addFieldReqs.toList: List[SchemaRequest.Update]).asJava)
      processResponse("add field request",
        req.process(solr))

      processResponse("commit",
        solr.commit())
    }

    solr.close()

    val sampleIdsBc = sc.broadcast(vds.sampleIds)
    val sampleAnnotationsBc = sc.broadcast(vds.sampleAnnotations)

    vds.rdd.foreachPartition {
      it =>
        val ab = mutable.ArrayBuilder.make[AnyRef]
        val documents = it.map {
          case (v, (va, gs)) =>

            val document = new SolrInputDocument()

            vparsed.foreach {
              case (name, spec, t, f) =>
                vEC.setAll(v, va)
                f().foreach(x => documentAddField(document, escapeString(name), t, x))
            }

            gs.iterator.zipWithIndex.foreach {
              case (g, i) =>
                if ((exportMissing || g.isCalled) && (exportRef || !g.isHomRef)) {
                  val s = sampleIdsBc.value(i)
                  val sa = sampleAnnotationsBc.value(i)
                  gparsed.foreach {
                    case (name, spec, t, f) =>
                      gEC.setAll(v, va, s, sa, g)
                      // __ can't appear in escaped string
                      f().foreach(x => documentAddField(document, escapeString(s) + "__" + escapeString(name), t, x))
                  }
                }
            }

            document
        }

        val solr =
          if (url != null)
            new HttpSolrClient.Builder(url + "/" + collection)
              .build()
          else {
            val cc = new CloudSolrClient.Builder()
              .withZkHost(zkHost)
              .build()
            cc.setDefaultCollection(collection)
            cc
          }

        var retry = true
        var retryInterval = 3 * 1000 // 3s
      val maxRetryInterval = 3 * 60 * 1000 // 3m

        while (retry) {
          try {
            processResponse("add documents",
              solr.add(documents.asJava))
            processResponse("commit",
              solr.commit())
            solr.close()
            retry = false
          } catch {
            case e: SolrException =>
              warn(s"exportvariantssolr: caught exception while adding documents: ${
                Main.expandException(e)
              }\n\tretrying")

              Thread.sleep(Random.nextInt(retryInterval))
              retryInterval = (retryInterval * 2).max(maxRetryInterval)
          }
        }
    }

    state
  }
}
