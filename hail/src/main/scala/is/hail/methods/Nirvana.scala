package is.hail.methods

import java.io.{FileInputStream, IOException}
import java.util.Properties

import is.hail.annotations._
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.ir.{I, TableLiteral, TableValue}
import is.hail.expr.types._
import is.hail.expr.types.physical.PType
import is.hail.expr.types.virtual._
import is.hail.rvd.{RVD, RVDContext}
import is.hail.sparkextras.ContextRDD
import is.hail.table.Table
import is.hail.utils._
import is.hail.variant.{Locus, RegionValueVariant}
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._


object Nirvana {

  //For Nirnava v2.0.8

  val nirvanaSignature = TStruct(
    I("chromosome") -> TString(),
    I("refAllele") -> TString(),
    I("position") -> TInt32(),
    I("altAlleles") -> TArray(TString()),
    I("cytogeneticBand") -> TString(),
    I("quality") -> TFloat64(),
    I("filters") -> TArray(TString()),
    I("jointSomaticNormalQuality") -> TInt32(),
    I("copyNumber") -> TInt32(),
    I("strandBias") -> TFloat64(),
    I("recalibratedQuality") -> TFloat64(),
    I("clingen") -> TArray(TStruct(
      I("chromosome") -> TString(),
      I("begin") -> TInt32(),
      I("end") -> TInt32(),
      I("variantType") -> TString(),
      I("id") -> TString(),
      I("clinicalInterpretation") -> TString(),
      I("observedGains") -> TInt32(),
      I("observedLosses") -> TInt32(),
      I("validated") -> TBoolean(),
      I("phenotypes") -> TArray(TString()),
      I("phenotypeIds") -> TArray(TString()),
      I("reciprocalOverlap") -> TFloat64()
    )),
    I("dgv") -> TArray(TStruct(
      I("chromosome") -> TString(),
      I("begin") -> TInt32(),
      I("end") -> TInt32(),
      I("variantType") -> TString(),
      I("id") -> TString(),
      I("variantFreqAll") -> TFloat64(),
      I("sampleSize") -> TInt32(),
      I("observedGains") -> TInt32(),
      I("observedLosses") -> TInt32(),
      I("reciprocalOverlap") -> TFloat64()
    )),
    I("oneKg") -> TArray(TStruct(
      I("chromosome") -> TString(),
      I("begin") -> TInt32(),
      I("end") -> TInt32(),
      I("variantType") -> TString(),
      I("id") -> TString(),
      I("variantFreqAll") -> TFloat64(),
      I("variantFreqAfr") -> TFloat64(),
      I("variantFreqAmr") -> TFloat64(),
      I("variantFreqEas") -> TFloat64(),
      I("variantFreqEur") -> TFloat64(),
      I("variantFreqSas") -> TFloat64(),
      I("sampleSize") -> TInt32(),
      I("sampleSizeAfr") -> TInt32(),
      I("sampleSizeAmr") -> TInt32(),
      I("sampleSizeEas") -> TInt32(),
      I("sampleSizeEur") -> TInt32(),
      I("sampleSizeSas") -> TInt32(),
      I("observedGains") -> TInt32(),
      I("observedLosses") -> TInt32(),
      I("reciprocalOverlap") -> TFloat64()
    )),
    I("cosmic") -> TArray(TStruct(
      I("id") -> TInt32(),
      I("chromosome") -> TString(),
      I("begin") -> TInt32(),
      I("end") -> TInt32(),
      I("variantType") -> TString(),
      I("copyNumber") -> TInt32(),
      I("cancerTypes") -> TArray(TTuple(TString(),TInt32())),
      I("tissues") -> TArray(TTuple(TString(),TInt32())),
      I("reciprocalOverlap") -> TFloat64()
    )),
    I("variants") -> TArray(TStruct(
      I("altAllele") -> TString(),
      I("refAllele") -> TString(),
      I("chromosome") -> TString(),
      I("begin") -> TInt32(),
      I("end") -> TInt32(),
      I("phylopScore") -> TFloat64(),
      I("isReferenceMinor") -> TBoolean(),
      I("variantType") -> TString(),
      I("vid") -> TString(),
      I("hgvsg") -> TString(),
      I("isRecomposedVariant") -> TBoolean(),
      I("isDecomposedVariant") -> TBoolean(),
      I("regulatoryRegions") -> TArray(TStruct(
        I("id") -> TString(),
        I("type") -> TString(),
        I("consequence") -> TSet(TString())
      )),
      I("clinvar") -> TArray(TStruct(
        I("id") -> TString(),
        I("reviewStatus") -> TString(),
        I("isAlleleSpecific") -> TBoolean(),
        I("alleleOrigins") -> TArray(TString()),
        I("refAllele") -> TString(),
        I("altAllele") -> TString(),
        I("phenotypes") -> TArray(TString()),
        I("medGenIds") -> TArray(TString()),
        I("omimIds") -> TArray(TString()),
        I("orphanetIds") -> TArray(TString()),
        I("significance") -> TString(),
        I("lastUpdatedDate") -> TString(),
        I("pubMedIds") -> TArray(TString())
      )),
      I("cosmic") -> TArray(TStruct(
        I("id") -> TString(),
        I("isAlleleSpecific") -> TBoolean(),
        I("refAllele") -> TString(),
        I("altAllele") -> TString(),
        I("gene") -> TString(),
        I("sampleCount") -> TInt32(),
        I("studies") -> TArray(TStruct(
          I("id") -> TInt32(),
          I("histology") -> TString(),
          I("primarySite") -> TString()
        ))
      )),
      I("dbsnp") -> TStruct(I("ids") -> TArray(TString())),
      I("gnomad") -> TStruct(
        I("coverage") -> TString(),
        I("allAf") -> TFloat64(),
        I("allAc") -> TInt32(),
        I("allAn") -> TInt32(),
        I("allHc") -> TInt32(),
        I("afrAf") -> TFloat64(),
        I("afrAc") -> TInt32(),
        I("afrAn") -> TInt32(),
        I("afrHc") -> TInt32(),
        I("amrAf") -> TFloat64(),
        I("amrAc") -> TInt32(),
        I("amrAn") -> TInt32(),
        I("amrHc") -> TInt32(),
        I("easAf") -> TFloat64(),
        I("easAc") -> TInt32(),
        I("easAn") -> TInt32(),
        I("easHc") -> TInt32(),
        I("finAf") -> TFloat64(),
        I("finAc") -> TInt32(),
        I("finAn") -> TInt32(),
        I("finHc") -> TInt32(),
        I("nfeAf") -> TFloat64(),
        I("nfeAc") -> TInt32(),
        I("nfeAn") -> TInt32(),
        I("nfeHc") -> TInt32(),
        I("othAf") -> TFloat64(),
        I("othAc") -> TInt32(),
        I("othAn") -> TInt32(),
        I("othHc") -> TInt32(),
        I("asjAf") -> TFloat64(),
        I("asjAc") -> TInt32(),
        I("asjAn") -> TInt32(),
        I("asjHc") -> TInt32(),
        I("failedFilter") -> TBoolean()
      ),
      I("gnomadExome") -> TStruct(
        I("coverage") -> TString(),
        I("allAf") -> TFloat64(),
        I("allAc") -> TInt32(),
        I("allAn") -> TInt32(),
        I("allHc") -> TInt32(),
        I("afrAf") -> TFloat64(),
        I("afrAc") -> TInt32(),
        I("afrAn") -> TInt32(),
        I("afrHc") -> TInt32(),
        I("amrAf") -> TFloat64(),
        I("amrAc") -> TInt32(),
        I("amrAn") -> TInt32(),
        I("amrHc") -> TInt32(),
        I("easAf") -> TFloat64(),
        I("easAc") -> TInt32(),
        I("easAn") -> TInt32(),
        I("easHc") -> TInt32(),
        I("finAf") -> TFloat64(),
        I("finAc") -> TInt32(),
        I("finAn") -> TInt32(),
        I("finHc") -> TInt32(),
        I("nfeAf") -> TFloat64(),
        I("nfeAc") -> TInt32(),
        I("nfeAn") -> TInt32(),
        I("nfeHc") -> TInt32(),
        I("othAf") -> TFloat64(),
        I("othAc") -> TInt32(),
        I("othAn") -> TInt32(),
        I("othHc") -> TInt32(),
        I("asjAf") -> TFloat64(),
        I("asjAc") -> TInt32(),
        I("asjAn") -> TInt32(),
        I("asjHc") -> TInt32(),
        I("sasAf") -> TFloat64(),
        I("sasAc") -> TInt32(),
        I("sasAn") -> TInt32(),
        I("sasHc") -> TInt32(),
        I("failedFilter") -> TBoolean()
      ),
      I("topmed") -> TStruct(
        I("failedFilter") -> TBoolean(),
        I("allAc") -> TInt32(),
        I("allAn") -> TInt32(),
        I("allAf") -> TFloat64(),
        I("allHc") -> TInt32()
      ),
      I("globalAllele") -> TStruct(
        I("globalMinorAllele") -> TString(),
        I("globalMinorAlleleFrequency") -> TFloat64()
      ),
      I("oneKg") -> TStruct(
        I("ancestralAllele") -> TString(),
        I("allAf") -> TFloat64(),
        I("allAc") -> TInt32(),
        I("allAn") -> TInt32(),
        I("afrAf") -> TFloat64(),
        I("afrAc") -> TInt32(),
        I("afrAn") -> TInt32(),
        I("amrAf") -> TFloat64(),
        I("amrAc") -> TInt32(),
        I("amrAn") -> TInt32(),
        I("easAf") -> TFloat64(),
        I("easAc") -> TInt32(),
        I("easAn") -> TInt32(),
        I("eurAf") -> TFloat64(),
        I("eurAc") -> TInt32(),
        I("eurAn") -> TInt32(),
        I("sasAf") -> TFloat64(),
        I("sasAc") -> TInt32(),
        I("sasAn") -> TInt32()
      ),
      I("mitomap") -> TArray(TStruct(
        I("refAllele") -> TString(),
        I("altAllele") -> TString(),
        I("diseases")  -> TArray(TString()),
        I("hasHomoplasmy") -> TBoolean(),
        I("hasHeteroplasmy") -> TBoolean(),
        I("status") -> TString(),
        I("clinicalSignificance") -> TString(),
        I("scorePercentile") -> TFloat64(),
        I("isAlleleSpecific") -> TBoolean(),
        I("chromosome") -> TString(),
        I("begin") -> TInt32(),
        I("end") -> TInt32(),
        I("variantType") -> TString()
      )),
      I("transcripts") -> TStruct(
        I("refSeq") -> TArray(TStruct(
          I("transcript") -> TString(),
          I("bioType") -> TString(),
          I("aminoAcids") -> TString(),
          I("cdnaPos") -> TString(),
          I("codons") -> TString(),
          I("cdsPos") -> TString(),
          I("exons") -> TString(),
          I("introns") -> TString(),
          I("geneId") -> TString(),
          I("hgnc") -> TString(),
          I("consequence") -> TArray(TString()),
          I("hgvsc") -> TString(),
          I("hgvsp") -> TString(),
          I("isCanonical") -> TBoolean(),
          I("polyPhenScore") -> TFloat64(),
          I("polyPhenPrediction") -> TString(),
          I("proteinId") -> TString(),
          I("proteinPos") -> TString(),
          I("siftScore") -> TFloat64(),
          I("siftPrediction") -> TString()
        )),
        I("ensembl") -> TArray(TStruct(
          I("transcript") -> TString(),
          I("bioType") -> TString(),
          I("aminoAcids") -> TString(),
          I("cdnaPos") -> TString(),
          I("codons") -> TString(),
          I("cdsPos") -> TString(),
          I("exons") -> TString(),
          I("introns") -> TString(),
          I("geneId") -> TString(),
          I("hgnc") -> TString(),
          I("consequence") -> TArray(TString()),
          I("hgvsc") -> TString(),
          I("hgvsp") -> TString(),
          I("isCanonical") -> TBoolean(),
          I("polyPhenScore") -> TFloat64(),
          I("polyPhenPrediction") -> TString(),
          I("proteinId") -> TString(),
          I("proteinPos") -> TString(),
          I("siftScore") -> TFloat64(),
          I("siftPrediction") -> TString()
        ))
      ),
      I("overlappingGenes") -> TArray(TString())
    )),
    I("genes") -> TArray(TStruct(
      I("name") -> TString(),
      I("omim") -> TArray(TStruct(
        I("mimNumber") -> TInt32(),
        I("hgnc") -> TString(),
        I("description") -> TString(),
        I("phenotypes") -> TArray(TStruct(
          I("mimNumber") -> TInt32(),
          I("phenotype") -> TString(),
          I("mapping") -> TString(),
          I("inheritance") -> TArray(TString()),
          I("comments") -> TString()
        ))
      )),
      I("exac") -> TStruct(
        I("pLi") -> TFloat64(),
        I("pRec") -> TFloat64(),
        I("pNull") -> TFloat64()
      )
    ))
  )

  def printContext(w: (String) => Unit) {
    w("##fileformat=VCFv4.1")
    w("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT")
  }

  def printElement(vaSignature: PType)(w: (String) => Unit, v: (Locus, Array[String])) {
    val (locus, alleles) = v

    val sb = new StringBuilder()
    sb.append(locus.contig)
    sb += '\t'
    sb.append(locus.position)
    sb.append("\t.\t")
    sb.append(alleles(0))
    sb += '\t'
    sb.append(alleles.tail.filter(_ != "*").mkString(","))
    sb += '\t'
    sb.append("\t.\t.\tGT")
    w(sb.result())
  }

  def annotate(ht: Table, config: String, blockSize: Int): Table = {
    assert(ht.key.contains(FastIndexedSeq("locus", "alleles")))
    assert(ht.typ.rowType.size == 2)

    val properties = try {
      val p = new Properties()
      val is = new FileInputStream(config)
      p.load(is)
      is.close()
      p
    } catch {
      case e: IOException =>
        fatal(s"could not open file: ${ e.getMessage }")
    }

    val dotnet = properties.getProperty("hail.nirvana.dotnet", "dotnet")

    val nirvanaLocation = properties.getProperty("hail.nirvana.location")
    if (nirvanaLocation == null)
      fatal("property `hail.nirvana.location' required")

    val path = Option(properties.getProperty("hail.nirvana.path"))

    val cache = properties.getProperty("hail.nirvana.cache")


    val supplementaryAnnotationDirectoryOpt = Option(properties.getProperty("hail.nirvana.supplementaryAnnotationDirectory"))
    val supplementaryAnnotationDirectory = if (supplementaryAnnotationDirectoryOpt.isEmpty) List[String]() else List("--sd", supplementaryAnnotationDirectoryOpt.get)

    val reference = properties.getProperty("hail.nirvana.reference")

    val cmd: List[String] = List[String](dotnet, s"$nirvanaLocation") ++
      List("-c", cache) ++
      supplementaryAnnotationDirectory ++
      List("--disable-recomposition", "-r", reference,
        "-i", "-",
        "-o", "-")

    println(cmd.mkString(" "))

    val contigQuery: Querier = nirvanaSignature.query(I("chromosome"))
    val startQuery = nirvanaSignature.query(I("position"))
    val refQuery = nirvanaSignature.query(I("refAllele"))
    val altsQuery = nirvanaSignature.query(I("altAlleles"))
    val localRowType = ht.typ.rowType.physicalType
    val localBlockSize = blockSize

    val rowKeyOrd = ht.typ.keyType.ordering

    info("Running Nirvana")

    val prev = ht.value.rvd

    val annotations = prev
      .mapPartitions { it =>
        val pb = new ProcessBuilder(cmd.asJava)
        val env = pb.environment()
        if (path.orNull != null)
          env.put("PATH", path.get)

        val rvv = new RegionValueVariant(localRowType)

        it.map { rv =>
          rvv.setRegion(rv)
          (rvv.locus(), rvv.alleles())
        }
          .grouped(localBlockSize)
          .flatMap { block =>
            val (jt, proc) = block.iterator.pipe(pb,
              printContext,
              printElement(localRowType),
              _ => ())
            // The filter is because every other output line is a comma.
            val kt = jt.filter(_.startsWith("{\"chromosome")).map { s =>
              val a = JSONAnnotationImpex.importAnnotation(JsonMethods.parse(s), nirvanaSignature)
              val locus = Locus(contigQuery(a).asInstanceOf[String],
                startQuery(a).asInstanceOf[Int])
              val alleles = refQuery(a).asInstanceOf[String] +: altsQuery(a).asInstanceOf[IndexedSeq[String]]
              (Annotation(locus, alleles), a)
            }

            val r = kt.toArray
              .sortBy(_._1)(rowKeyOrd.toOrdering)

            val rc = proc.waitFor()
            if (rc != 0)
              fatal(s"nirvana command failed with non-zero exit status $rc")

            r
          }
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    val nirvanaRVDType = prev.typ.copy(rowType = (ht.typ.rowType ++ TStruct(I("nirvana") -> nirvanaSignature)).physicalType)

    val nirvanaRowType = nirvanaRVDType.rowType

    val nirvanaRVD: RVD = RVD(
      nirvanaRVDType,
      prev.partitioner,
      ContextRDD.weaken[RVDContext](annotations).cmapPartitions { (ctx, it) =>
        val region = ctx.region
        val rvb = new RegionValueBuilder(region)
        val rv = RegionValue(region)

        it.map { case (v, nirvana) =>
          rvb.start(nirvanaRowType)
          rvb.startStruct()
          rvb.addAnnotation(nirvanaRowType.types(0).virtualType, v.asInstanceOf[Row].get(0))
          rvb.addAnnotation(nirvanaRowType.types(1).virtualType, v.asInstanceOf[Row].get(1))
          rvb.addAnnotation(nirvanaRowType.types(2).virtualType, nirvana)
          rvb.endStruct()
          rv.setOffset(rvb.end())

          rv
        }
      }).persist(StorageLevel.MEMORY_AND_DISK)

    new Table(ht.hc, TableLiteral(
      TableValue(
        TableType(nirvanaRowType.virtualType, FastIndexedSeq(I("locus"), I("alleles")), TStruct()),
        BroadcastRow(Row(), TStruct(), ht.hc.sc),
        nirvanaRVD
      )))
  }

  def apply(ht: Table, config: String, blockSize: Int = 500000): Table =
    annotate(ht, config, blockSize)
}
