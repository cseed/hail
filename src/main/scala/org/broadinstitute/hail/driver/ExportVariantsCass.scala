package org.broadinstitute.hail.driver

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import org.broadinstitute.hail.utils._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.utils.StringEscapeUtils._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

object CassandraStuff {
  private var cluster: Cluster = null
  private var session: Session = null

  private var refcount: Int = 0

  def getSession(address: String): Session = {
    this.synchronized {
      if (cluster == null)
        cluster = Cluster.builder()
          .addContactPoint(address)
          .build()

      if (session == null)
        session = cluster.connect()

      refcount += 1
    }

    session
  }

  def disconnect() {
    this.synchronized {
      refcount -= 1
      if (refcount == 0) {
        session.close()
        cluster.close()

        session = null
        cluster = null
      }
    }
  }
}

object ExportVariantsCass extends Command {

  class Options extends BaseOptions {

    @Args4jOption(required = true, name = "-a", aliases = Array("--address"),
      usage = "Cassandra contact point to connect to")
    var address: String = _

    @Args4jOption(name = "--export-ref", usage = "export HomRef calls")
    var exportRef = false

    @Args4jOption(name = "--export-missing", usage = "export missing genotypes")
    var exportMissing = false

    @Args4jOption(required = true, name = "-g",
      usage = "comma-separated list of fields/computations to be exported")
    var genotypeCondition: String = _

    @Args4jOption(required = true, name = "-k",
      usage = "Cassandra keyspace")
    var keyspace: String = _

    @Args4jOption(required = true, name = "-t", aliases = Array("--table"),
      usage = "Cassandra table")
    var table: String = _

    @Args4jOption(required = true, name = "-v",
      usage = "comma-separated list of fields/computations to be exported")
    var variantCondition: String = _

    @Args4jOption(name = "-d", aliases = Array("--drop"),
      usage = "drop and re-create cassandra table before exporting")
    var drop: Boolean = false

    @Args4jOption(name = "--block-size", usage = "Variants per SolrClient.add")
    var blockSize = 100
  }

  def newOptions = new Options

  def name = "exportvariantscass"

  def description = "Export variant information to Cassandra"

  def supportsMultiallelic = true

  def requiresVDS = true

  def toCassType(t: Type): String = t match {
    case TBoolean => "boolean"
    case TInt => "int"
    case TLong => "bigint"
    case TFloat => "float"
    case TDouble => "double"
    case TString => "text"
    case TArray(elementType) => s"list<${ toCassType(elementType) }>"
    case TSet(elementType) => s"set<${ toCassType(elementType) }>"
    case _ =>
      fatal("unsupported type: $t")
  }

  def toCassValue(a: Option[Any], t: Type): AnyRef = t match {
    case TArray(elementType) => a.map(_.asInstanceOf[Seq[_]].asJava).orNull
    case TSet(elementType) => a.map(_.asInstanceOf[Set[_]].asJava).orNull
    case _ => a.map(_.asInstanceOf[AnyRef]).orNull
  }

  def escapeString(name: String): String =
    escapeStringSimple(name, '_', !_.isLetter, !_.isLetterOrDigit)

  def escapeCassColumnName(name: String): String = {
    val sb = new StringBuilder

    if (name.head.isDigit)
      sb += 'x'

    name.foreach { c =>
      if (c.isLetterOrDigit)
        sb += c.toLower
      else
        sb += '_'
    }

    sb.result()
  }

  def run(state: State, options: Options): State = {
    val vds = state.vds
    val sc = vds.sparkContext
    val vas = vds.vaSignature
    val sas = vds.saSignature
    val gCond = options.genotypeCondition
    val vCond = options.variantCondition
    val address = options.address
    val exportRef = options.exportRef
    val exportMissing = options.exportMissing
    val drop = options.drop

    val keyspace = options.keyspace
    val table = options.table
    val qualifiedTable = keyspace + "." + table

    val vSymTab = Map(
      "v" -> (0, TVariant),
      "va" -> (1, vas))
    val vEC = EvalContext(vSymTab)
    val vA = vEC.a

    val (vNames, vTypes, vf) = Parser.parseNamedExprs(vCond, vEC)

    val gSymTab = Map(
      "v" -> (0, TVariant),
      "va" -> (1, vas),
      "s" -> (2, TSample),
      "sa" -> (3, sas),
      "g" -> (4, TGenotype))
    val gEC = EvalContext(gSymTab)
    val gA = gEC.a

    val (gHeader, gTypes, gf) = Parser.parseNamedExprs(gCond, gEC)

    val symTab = Map(
      "v" -> (0, TVariant),
      "va" -> (1, vas))
    val ec = EvalContext(symTab)

    val fields = vNames.map(escapeString).zip(vTypes) ++ vds.sampleIds.flatMap { s =>
      gHeader.map(field => s"${ escapeString(s) }__${ escapeString(field) }").zip(gTypes)
    }

    val session = CassandraStuff.getSession(address)

    // get keyspace (create it if it doesn't exist)
    var keyspaceMetadata = session.getCluster.getMetadata.getKeyspace(keyspace)
    if (keyspaceMetadata == null) {
      info(s"creating keyspace ${ keyspace }")
      try {
        session.execute(s"CREATE KEYSPACE ${ keyspace } " +
          "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true")
      } catch {
        case e: Exception => fatal(s"exportvariantscass: unable to create keyspace ${ keyspace }: ${ e }")
      }
      keyspaceMetadata = session.getCluster.getMetadata.getKeyspace(keyspace)
    }

    // get table (drop and create it if necessary)
    if (drop) {
      info(s"dropping table ${ qualifiedTable }")
      try {
        session.execute(SchemaBuilder.dropTable(keyspace, table).ifExists());
      } catch {
        case e: Exception => warn(s"exportvariantscass: unable to drop table ${ qualifiedTable }: ${ e }")
      }
    }

    var tableMetadata = keyspaceMetadata.getTable(table)
    if (tableMetadata == null) {
      info(s"creating table ${ qualifiedTable }")
      try {
        session.execute(s"CREATE TABLE $qualifiedTable (${ escapeString("dataset_id") } text, chrom text, start int, ref text, alt text, " +
          s"PRIMARY KEY ((${ escapeString("dataset_id") }, chrom, start), ref, alt))") // WITH COMPACT STORAGE")
      } catch {
        case e: Exception => fatal(s"exportvariantscass: unable to create table ${ qualifiedTable }: ${ e }")
      }
      //info(s"table ${qualifiedTable} created")
      tableMetadata = keyspaceMetadata.getTable(table)
    }

    val preexistingFields = tableMetadata.getColumns.asScala.map(_.getName).toSet
    val toAdd = fields
      .filter { case (name, t) => !preexistingFields(name) }

    if (toAdd.nonEmpty) {
      session.execute(s"ALTER TABLE $qualifiedTable ADD (${
        toAdd.map { case (name, t) => s""""$name" ${ toCassType(t) }""" }.mkString(",")
      })")
    }

    CassandraStuff.disconnect()

    val sampleIdsBc = sc.broadcast(vds.sampleIds)
    val sampleAnnotationsBc = sc.broadcast(vds.sampleAnnotations)
    val localBlockSize = options.blockSize
    val maxRetryInterval = 3 * 60 * 1000 // 3m

    val futures = vds.rdd
      .foreachPartition { it =>
        val session = CassandraStuff.getSession(address)
        val nb = new mutable.ArrayBuffer[String]
        val vb = new mutable.ArrayBuffer[AnyRef]

        it
          .grouped(localBlockSize)
          .foreach { block =>

            var retryInterval = 3 * 1000 // 3s

            var toInsert = block.map { case (v, (va, gs)) =>
              nb.clear()
              vb.clear()

              vEC.setAll(v, va)
              vf().zipWithIndex.foreach { case (a, i) =>
                nb += s""""${ escapeString(vNames(i)) }""""
                vb += toCassValue(a, vTypes(i))
              }

              gs.iterator.zipWithIndex.foreach { case (g, i) =>
                val s = sampleIdsBc.value(i)
                val sa = sampleAnnotationsBc.value(i)
                if ((exportMissing || g.isCalled) && (exportRef || !g.isHomRef)) {
                  gEC.setAll(v, va, s, sa, g)
                  gf().zipWithIndex.foreach { case (a, j) =>
                    nb += s""""${ escapeString(s) }__${ escapeString(gHeader(j)) }""""
                    vb += toCassValue(a, gTypes(j))
                  }
                }
              }

              (nb.toArray, vb.toArray)
            }

            while (toInsert.nonEmpty) {
              toInsert = toInsert.map { case nv@(names, values) =>
                val future = session.executeAsync(QueryBuilder
                  .insertInto(keyspace, table)
                  .values(names, values))

                (nv, future)
              }.flatMap { case (nv, future) =>
                try {
                  future.getUninterruptibly()
                  None
                } catch {
                  case t: Throwable =>
                    warn(s"caught exception while adding inserting: ${
                      Main.expandException(t)
                    }\n\tretrying")

                    Some(nv)
                }
              }
            }

            if (toInsert.nonEmpty) {
              Thread.sleep(Random.nextInt(retryInterval))
              retryInterval = (retryInterval * 2).max(maxRetryInterval)
            }
          }

        CassandraStuff.disconnect()
      }

    state
  }
}
