package org.broadinstitute.hail.variant

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr.SparkAnnotationImpex
import org.broadinstitute.hail.sparkextras.{OrderedPartitioner, OrderedRDD}
import org.broadinstitute.hail.utils._
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ArrayBuffer

object VariantIndexedVDS {
  def apply(sqlContext: SQLContext, path: String): VariantIndexedVDS = {
    val sc = sqlContext.sparkContext
    val hConf = sc.hadoopConfiguration

    val metadata = VariantSampleMatrix.readMetadata(hConf, path)

    val jv = hConf.readFile(path + "/partitioner.json.gz")(JsonMethods.parse(_))
    val orderedPartitioner = jv.fromJSON[OrderedPartitioner[Locus, Variant]]

    val dfs = hConf.globAll(Array(path + "/rdd.parquet/*.parquet"))
      .map { partPath =>
        val part = getParquetPartNumber(partPath)
        (part, sqlContext.read.parquet(partPath))
      }.sortBy(_._1)
      .map(_._2)

    // cache
    dfs.reduce(_.union(_)).count()

    new VariantIndexedVDS(sqlContext, metadata, orderedPartitioner, dfs)
  }
}

class VariantIndexedVDS(
  sqlContext: SQLContext,
  metadata: VariantMetadata,
  orderedPartitioner: OrderedPartitioner[Locus, Variant],
  dfs: Array[DataFrame]) {
  assert(dfs.length == orderedPartitioner.numPartitions)

  private def sc = sqlContext.sparkContext

  def numPartitions: Int = orderedPartitioner.numPartitions

  private val vaRequiresConversion = SparkAnnotationImpex.requiresConversion(metadata.vaSignature)

  def query(variantList: String): VariantDataset = {
    val variants = sc.textFileLines(variantList)
      .map {
        _.map { line =>
          val fields = line.split(":")
          if (fields.length != 4)
            fatal("invalid variant: expect `CHR:POS:REF:ALT1,ALT2,...,ALTN'")
          val ref = fields(2)
          Variant(fields(0),
            fields(1).toInt,
            ref,
            fields(3).split(",").map(alt => AltAllele(ref, alt)))
        }.value
      }.collect()

    query(variants)
  }

  def query(vs: Array[Variant]): VariantDataset = {
    val pab = Array.fill(numPartitions)(new ArrayBuffer[Variant])

    vs.foreach { v =>
      val p = orderedPartitioner.getPartition(v)
      pab(p) += v
    }

    val partVariants = pab.zipWithIndex.map { case (ab, i) =>
      i -> ab
    }.filter(_._2.nonEmpty)

    val orderedRDD: OrderedRDD[Locus, Variant, (Annotation, Iterable[Genotype])] =
      if (partVariants.isEmpty)
        OrderedRDD.empty(sc)
      else {
        val resultDF = partVariants.map { case (i, ab) =>
          val df = dfs(i)
          df.filter(
            ab.map(v => df("contig").equalTo(v.contig) && df("start").equalTo(v.start))
              .reduce(_ || _))
        }.reduce(_.union(_))

        val variantsSetBc = sc.broadcast(vs.toSet)

        val localVARequiresConversion = vaRequiresConversion
        val localVASignature = metadata.vaSignature
        val localIsDosage = metadata.isDosage

        val rdd = resultDF
          .select("variant", "annotations", "gs")
          .rdd
          .map { row =>
            val v = row.getVariant(0)
            (v,
              (if (localVARequiresConversion)
                SparkAnnotationImpex.importAnnotation(row.get(1), localVASignature)
              else
                row.get(1),
                row.getGenotypeStream(v, 2, localIsDosage): Iterable[Genotype]))
          }
          .filter { case (v, (va, gs)) =>
            variantsSetBc.value(v)
          }

        OrderedRDD(rdd,
          OrderedPartitioner(
            partVariants.init.map { case (i, ab) =>
                orderedPartitioner.rangeBounds(i)
            },
          partVariants.length))
      }

    new VariantDataset(metadata, orderedRDD)
  }
}
