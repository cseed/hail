package org.broadinstitute.hail.driver

import org.broadinstitute.hail.expr.{EvalContext, _}
import org.broadinstitute.hail.io.TextExporter
import org.broadinstitute.hail.keytable.KeyTable
import org.broadinstitute.hail.utils._
import org.kohsuke.args4j.{Option => Args4jOption}

object ExportKeyTable extends Command with TextExporter {

  class Options extends BaseOptions {

    @Args4jOption(required = true, name = "-o", aliases = Array("--output"),
      usage = "path of output tsv")
    var output: String = _

    @Args4jOption(required = true, name = "-n", aliases = Array("--name"),
      usage = "name of key table to be printed to tsv")
    var name: String = _

    @Args4jOption(required = false, name = "-t", aliases = Array("--types"),
      usage = "Write the types of parse expressions to a file at the given path")
    var typesFile: String = _

  }

  def newOptions = new Options

  def name = "exportkeytable"

  def description = "Export key table to tsv"

  def supportsMultiallelic = true

  def requiresVDS = false

  override def hidden = true

  def run(state: State, options: Options): State = {

    val kt = state.ktEnv.get(options.name) match {
      case Some(newKT) =>
        newKT
      case None =>
        fatal("no such key table $name in environment")
    }

    val output = options.output

    val symTab = kt.fields.zipWithIndex.map{case (fd, i) => (fd.name, (i, fd.`type`))}.toMap

    val ec = EvalContext(symTab)

    val (header, types, f) = Parser.parseNamedArgs(kt.fieldNames.map(n => n + " = " + n).mkString(","), ec)

    Option(options.typesFile).foreach { file =>
      val typeInfo = header
        .getOrElse(types.indices.map(i => s"_$i").toArray)
        .zip(types)
      exportTypes(file, state.hadoopConf, typeInfo)
    }

    state.hadoopConf.delete(output, recursive = true)

    val nKeys = kt.nKeys

    kt.rdd
      .mapPartitions { it =>
        val sb = new StringBuilder()
        it.map { case (k, v) =>
          sb.clear()
          KeyTable.setEvalContext(ec, k, v, nKeys)
          f().foreachBetween(x => sb.append(x))(sb += '\t')
          sb.result()
        }
      }.writeTable(output, header.map(_.mkString("\t")))

    state
  }
}

