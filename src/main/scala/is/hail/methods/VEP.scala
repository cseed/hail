package is.hail.methods

import java.io.{FileInputStream, IOException}
import java.util.Properties

import is.hail.annotations.{Annotation, Region, RegionValue, RegionValueBuilder}
import is.hail.expr._
import is.hail.expr.types._
import is.hail.rvd.{OrderedRVD, OrderedRVType}
import is.hail.utils._
import is.hail.variant.{MatrixTable, RegionValueVariant, Variant}
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._
import scala.collection.mutable

object VEP {

  val vepSignature = TStruct(
    "assembly_name" -> TString(),
    "allele_string" -> TString(),
    "ancestral" -> TString(),
    "colocated_variants" -> TArray(TStruct(
      "aa_allele" -> TString(),
      "aa_maf" -> TFloat64(),
      "afr_allele" -> TString(),
      "afr_maf" -> TFloat64(),
      "allele_string" -> TString(),
      "amr_allele" -> TString(),
      "amr_maf" -> TFloat64(),
      "clin_sig" -> TArray(TString()),
      "end" -> TInt32(),
      "eas_allele" -> TString(),
      "eas_maf" -> TFloat64(),
      "ea_allele" -> TString(),
      "ea_maf" -> TFloat64(),
      "eur_allele" -> TString(),
      "eur_maf" -> TFloat64(),
      "exac_adj_allele" -> TString(),
      "exac_adj_maf" -> TFloat64(),
      "exac_allele" -> TString(),
      "exac_afr_allele" -> TString(),
      "exac_afr_maf" -> TFloat64(),
      "exac_amr_allele" -> TString(),
      "exac_amr_maf" -> TFloat64(),
      "exac_eas_allele" -> TString(),
      "exac_eas_maf" -> TFloat64(),
      "exac_fin_allele" -> TString(),
      "exac_fin_maf" -> TFloat64(),
      "exac_maf" -> TFloat64(),
      "exac_nfe_allele" -> TString(),
      "exac_nfe_maf" -> TFloat64(),
      "exac_oth_allele" -> TString(),
      "exac_oth_maf" -> TFloat64(),
      "exac_sas_allele" -> TString(),
      "exac_sas_maf" -> TFloat64(),
      "id" -> TString(),
      "minor_allele" -> TString(),
      "minor_allele_freq" -> TFloat64(),
      "phenotype_or_disease" -> TInt32(),
      "pubmed" -> TArray(TInt32()),
      "sas_allele" -> TString(),
      "sas_maf" -> TFloat64(),
      "somatic" -> TInt32(),
      "start" -> TInt32(),
      "strand" -> TInt32())),
    "context" -> TString(),
    "end" -> TInt32(),
    "id" -> TString(),
    "input" -> TString(),
    "intergenic_consequences" -> TArray(TStruct(
      "allele_num" -> TInt32(),
      "consequence_terms" -> TArray(TString()),
      "impact" -> TString(),
      "minimised" -> TInt32(),
      "variant_allele" -> TString())),
    "most_severe_consequence" -> TString(),
    "motif_feature_consequences" -> TArray(TStruct(
      "allele_num" -> TInt32(),
      "consequence_terms" -> TArray(TString()),
      "high_inf_pos" -> TString(),
      "impact" -> TString(),
      "minimised" -> TInt32(),
      "motif_feature_id" -> TString(),
      "motif_name" -> TString(),
      "motif_pos" -> TInt32(),
      "motif_score_change" -> TFloat64(),
      "strand" -> TInt32(),
      "variant_allele" -> TString())),
    "regulatory_feature_consequences" -> TArray(TStruct(
      "allele_num" -> TInt32(),
      "biotype" -> TString(),
      "consequence_terms" -> TArray(TString()),
      "impact" -> TString(),
      "minimised" -> TInt32(),
      "regulatory_feature_id" -> TString(),
      "variant_allele" -> TString())),
    "seq_region_name" -> TString(),
    "start" -> TInt32(),
    "strand" -> TInt32(),
    "transcript_consequences" -> TArray(TStruct(
      "allele_num" -> TInt32(),
      "amino_acids" -> TString(),
      "biotype" -> TString(),
      "canonical" -> TInt32(),
      "ccds" -> TString(),
      "cdna_start" -> TInt32(),
      "cdna_end" -> TInt32(),
      "cds_end" -> TInt32(),
      "cds_start" -> TInt32(),
      "codons" -> TString(),
      "consequence_terms" -> TArray(TString()),
      "distance" -> TInt32(),
      "domains" -> TArray(TStruct(
        "db" -> TString(),
        "name" -> TString())),
      "exon" -> TString(),
      "gene_id" -> TString(),
      "gene_pheno" -> TInt32(),
      "gene_symbol" -> TString(),
      "gene_symbol_source" -> TString(),
      "hgnc_id" -> TString(),
      "hgvsc" -> TString(),
      "hgvsp" -> TString(),
      "hgvs_offset" -> TInt32(),
      "impact" -> TString(),
      "intron" -> TString(),
      "lof" -> TString(),
      "lof_flags" -> TString(),
      "lof_filter" -> TString(),
      "lof_info" -> TString(),
      "minimised" -> TInt32(),
      "polyphen_prediction" -> TString(),
      "polyphen_score" -> TFloat64(),
      "protein_end" -> TInt32(),
      "protein_start" -> TInt32(),
      "protein_id" -> TString(),
      "sift_prediction" -> TString(),
      "sift_score" -> TFloat64(),
      "strand" -> TInt32(),
      "swissprot" -> TString(),
      "transcript_id" -> TString(),
      "trembl" -> TString(),
      "uniparc" -> TString(),
      "variant_allele" -> TString())),
    "variant_class" -> TString())

  val consequenceIndices = Set(8, 10, 11, 15)

  def printContext(w: (String) => Unit) {
    w("##fileformat=VCFv4.1")
    w("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT")
  }

  def printElement(w: (String) => Unit, v: Variant) {
    val sb = new StringBuilder()
    sb.append(v.contig)
    sb += '\t'
    sb.append(v.start)
    sb.append("\t.\t")
    sb.append(v.ref)
    sb += '\t'
    sb.append(v.altAlleles.iterator.map(_.alt).filter(_ != "*").mkString(","))
    sb.append("\t.\t.\tGT")
    w(sb.result())
  }

  def variantFromInput(input: String): Variant = {
    val a = input.split("\t")
    Variant(a(0),
      a(1).toInt,
      a(3),
      a(4).split(","))
  }

  def getCSQHeaderDefinition(cmd: Array[String], perl5lib: String, path: String): Option[String] = {
    val csqHeaderRegex = "ID=CSQ[^>]+Description=\"([^\"]+)".r
    val pb = new ProcessBuilder(cmd.toList.asJava)
    val env = pb.environment()
    if (perl5lib != null)
      env.put("PERL5LIB", perl5lib)
    if (path != null)
      env.put("PATH", path)
    val (jt, proc) = List(Variant("1", 13372, "G", "C")).iterator.pipe(pb,
      printContext,
      printElement,
      _ => ())

    val csqHeader = jt.flatMap(s => csqHeaderRegex.findFirstMatchIn(s).map(m => m.group(1)))
    val rc = proc.waitFor()
    if (rc != 0)
      fatal(s"vep command failed with non-zero exit status $rc")

    if (csqHeader.hasNext)
      Some(csqHeader.next())
    else {
      warn("Could not get VEP CSQ header")
      None
    }
  }

  def getNonStarToOriginalAlleleIdxMap(v: Variant): mutable.Map[Int, Int] = {
    val alleleMap = mutable.Map[Int, Int]()
    (0 until v.nAltAlleles).foldLeft(0) {
      case (nStar, aai) =>
        if (v.altAlleles(aai).alt == "*")
          nStar + 1
        else {
          alleleMap(aai - nStar + 1) = aai + 1
          nStar
        }
    }
    alleleMap
  }

  def annotate(vsm: MatrixTable, config: String, root: String = "va.vep", csq: Boolean,
    blockSize: Int): MatrixTable = {

    val parsedRoot = Parser.parseAnnotationRoot(root, Annotation.VARIANT_HEAD)

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

    val perl = properties.getProperty("hail.vep.perl", "perl")

    val perl5lib = properties.getProperty("hail.vep.perl5lib")

    val path = properties.getProperty("hail.vep.path")

    val location = properties.getProperty("hail.vep.location")
    if (location == null)
      fatal("property `hail.vep.location' required")

    val cacheDir = properties.getProperty("hail.vep.cache_dir")
    if (cacheDir == null)
      fatal("property `hail.vep.cache_dir' required")


    val plugin = if (properties.getProperty("hail.vep.plugin") != null) {
      properties.getProperty("hail.vep.plugin")
    } else {

      val humanAncestor = properties.getProperty("hail.vep.lof.human_ancestor")
      if (humanAncestor == null)
        fatal("property `hail.vep.lof.human_ancestor' required")

      val conservationFile = properties.getProperty("hail.vep.lof.conservation_file")
      if (conservationFile == null)
        fatal("property `hail.vep.lof.conservation_file' required")

      s"LoF,human_ancestor_fa:$humanAncestor,filter_position:0.05,min_intron_size:15,conservation_file:$conservationFile"
    }

    val fasta = properties.getProperty("hail.vep.fasta")
    if (fasta == null)
      fatal("property `hail.vep.fasta' required")

    var assembly = properties.getProperty("hail.vep.assembly")
    if (assembly == null) {
      warn("property `hail.vep.assembly' not specified. Setting to GRCh37")
      assembly = "GRCh37"
    }

    val cmd =
      Array(
        perl,
        s"$location",
        "--format", "vcf",
        if (csq) "--vcf" else "--json",
        "--everything",
        "--allele_number",
        "--no_stats",
        "--cache", "--offline",
        "--dir", s"$cacheDir",
        "--fasta", s"$fasta",
        "--minimal",
        "--assembly", s"$assembly",
        "--plugin", s"$plugin",
        "-o", "STDOUT")

    val inputQuery = vepSignature.query("input")

    val csqRegex = "CSQ=[^;^\\t]+".r

    val localBlockSize = blockSize

    val csqHeader = if (csq) getCSQHeaderDefinition(cmd, perl5lib, path).getOrElse("") else ""
    val alleleNumIndex = if (csq) csqHeader.split("\\|").indexOf("ALLELE_NUM") else -1

    val localRowType = vsm.rvRowType
    val annotations = vsm.rvd
      .mapPartitions { it =>
        val pb = new ProcessBuilder(cmd.toList.asJava)
        val env = pb.environment()
        if (perl5lib != null)
          env.put("PERL5LIB", perl5lib)
        if (path != null)
          env.put("PATH", path)

        val rvv = new RegionValueVariant(localRowType)
        it
          .map { rv =>
            rvv.setRegion(rv)
            rvv.variantObject()
          }
          .grouped(localBlockSize)
          .flatMap { block =>
            val (jt, proc) = block.iterator.pipe(pb,
              printContext,
              printElement,
              _ => ())

            val nonStarToOriginalVariant = block.map { v =>
              (v.copy(altAlleles = v.altAlleles.filter(_.alt != "*")), v)
            }.toMap

            val kt = jt
              .filter(s => !s.isEmpty && s(0) != '#')
              .map { s =>
                if (csq) {
                  val vvep = variantFromInput(s)
                  if (!nonStarToOriginalVariant.contains(vvep))
                    fatal(s"VEP output variant ${ vvep } not found in original variants.\nVEP output: $s")

                  val v = nonStarToOriginalVariant(vvep)
                  val x = csqRegex.findFirstIn(s)
                  x match {
                    case Some(value) =>
                      val tr_aa = value.substring(4).split(",")
                      if (vvep != v && alleleNumIndex > -1) {
                        val alleleMap = getNonStarToOriginalAlleleIdxMap(v)
                        (v, tr_aa.map {
                          x =>
                            val xsplit = x.split("\\|")
                            val allele_num = xsplit(alleleNumIndex)
                            if (allele_num.isEmpty)
                              x
                            else
                              xsplit
                                .updated(alleleNumIndex, alleleMap.getOrElse(xsplit(alleleNumIndex).toInt, "").toString)
                                .mkString("|")
                        }: IndexedSeq[Annotation])
                      } else
                        (v, tr_aa: IndexedSeq[Annotation])
                    case None =>
                      warn(s"No VEP annotation found for variant $v. VEP returned $s.")
                      (v, IndexedSeq.empty[Annotation])
                  }
                } else {
                  val a = JSONAnnotationImpex.importAnnotation(JsonMethods.parse(s), vepSignature)
                  val vvep = variantFromInput(inputQuery(a).asInstanceOf[String])

                  if (!nonStarToOriginalVariant.contains(vvep))
                    fatal(s"VEP output variant ${ vvep } not found in original variants.\nVEP output: $s")

                  val v = nonStarToOriginalVariant(vvep)
                  if (vvep != v) {
                    val alleleMap = getNonStarToOriginalAlleleIdxMap(v)
                    (v, consequenceIndices.foldLeft(a.asInstanceOf[Row]) {
                      case (r, i) =>
                        if (r(i) == null)
                          r
                        else {
                          r.update(i, r(i).asInstanceOf[IndexedSeq[Row]].map {
                            x => x.update(0, alleleMap.getOrElse(x.getInt(0), null))
                          })
                        }
                    })
                  } else
                    (v, a)
                }
              }

            val r = kt.toArray
              .sortBy(_._1)(localRowType.fieldType(1).asInstanceOf[TVariant].gr.variantOrdering)

            val rc = proc.waitFor()
            if (rc != 0)
              fatal(s"vep command '${ cmd.mkString(" ") }' failed with non-zero exit status $rc")

            r
          }
      }.persist(StorageLevel.MEMORY_AND_DISK)

    val vepType: Type = if (csq) TArray(TString()) else vepSignature

    val vepOrderedRVType = new OrderedRVType(
      Array("locus"), Array("locus", "alleles"),
      TStruct(
        "locus" -> vsm.rowKeyTypes(0),
        "alleles" -> vsm.rowKeyTypes(1),
        "vep" -> vepType))

    val vepRowType = vepOrderedRVType.rowType

    val vepRVD: OrderedRVD = OrderedRVD(
      vepOrderedRVType,
      vsm.rvd.partitioner,
      annotations.mapPartitions { it =>
        val region = Region()
        val rvb = new RegionValueBuilder(region)
        val rv = RegionValue(region)

        it.map { case (v, vep) =>
          rvb.start(vepRowType)
          rvb.startStruct()
          rvb.addAnnotation(vepRowType.fieldType(0), v.locus)
          rvb.addAnnotation(vepRowType.fieldType(1), IndexedSeq(v.ref) ++ v.altAlleles.map(_.alt))
          rvb.addAnnotation(vepRowType.fieldType(2), vep)
          rvb.endStruct()
          rv.setOffset(rvb.end())

          rv
      }})

    info(s"vep: annotated ${ annotations.count() } variants")

    vsm.orderedRVDLeftJoinDistinctAndInsert(vepRVD, "vep", product = false)
      .annotateVariantsExpr("vep = vep.vep")
  }

  def apply(vsm: MatrixTable, config: String, root: String = "va.vep", csq: Boolean = false, blockSize: Int = 1000): MatrixTable =
    annotate(vsm, config, root, csq, blockSize)
}
