package is.hail.variant

import is.hail.HailContext
import org.apache.spark.sql.DataFrame
import is.hail.annotations.Annotation
import is.hail.expr.SparkAnnotationImpex
import is.hail.sparkextras.{OrderedPartitioner, OrderedRDD}
import is.hail.utils._
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ArrayBuffer

object VariantIndexedVDS {
  def apply(hc: HailContext, path: String): VariantIndexedVDS = {
    val (metadata, _) = VariantDataset.readMetadata(hc.hadoopConf, path)

    val jv = hc.hadoopConf.readFile(path + "/partitioner.json.gz")(JsonMethods.parse(_))
    val orderedPartitioner = jv.fromJSON[OrderedPartitioner[Locus, Variant]]

    val df = hc.sqlContext.read.parquet(path + "/rdd.parquet")
    println("vivds read partitions", df.rdd.partitions.length)
    df.count()

    new VariantIndexedVDS(hc, metadata, orderedPartitioner, df)
  }
}

class VariantIndexedVDS(
  hc: HailContext,
  val metadata: VariantMetadata,
  orderedPartitioner: OrderedPartitioner[Locus, Variant],
  df: DataFrame) {

  def numPartitions: Int = orderedPartitioner.numPartitions

  private val vaRequiresConversion = SparkAnnotationImpex.requiresConversion(metadata.vaSignature)

  def query(variantList: String): VariantDataset = {
    val variants = hc.sc.textFileLines(variantList)
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
        OrderedRDD.empty(hc.sc)
      else {
        val resultDF = partVariants.map { case (i, ab) =>
          val dfi = df.filter(df("part").equalTo(i))
          dfi.filter(
            ab.map(v => dfi("contig").equalTo(v.contig) && dfi("start").equalTo(v.start))
              .reduce(_ || _))
        }.reduce(_.union(_))
          .select("variant", "annotations", "gs")

        // resultDF.explain()

        val variantsSetBc = hc.sc.broadcast(vs.toSet)

        val localVARequiresConversion = vaRequiresConversion
        val localVASignature = metadata.vaSignature
        val localIsDosage = metadata.isDosage

        val rdd = resultDF
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

    new VariantDataset(hc, metadata, orderedRDD)
  }
}
