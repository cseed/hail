package is.hail.io.bgen

import is.hail.HailContext
import is.hail.expr.ir.I
import is.hail.expr.types.virtual.TStruct
import is.hail.io.index.IndexWriter
import is.hail.rvd.{RVD, RVDPartitioner, RVDType}
import is.hail.utils._
import is.hail.variant.ReferenceGenome
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

private case class IndexBgenPartition(
  path: String,
  compressed: Boolean,
  skipInvalidLoci: Boolean,
  contigRecoding: Map[String, String],
  startByteOffset: Long,
  endByteOffset: Long,
  partitionIndex: Int,
  sHadoopConfBc: Broadcast[SerializableHadoopConfiguration]
) extends BgenPartition {

  def index = partitionIndex
}

object IndexBgen {
  def apply(
    hc: HailContext,
    files: Array[String],
    indexFileMap: Map[String, String] = null,
    rg: Option[String] = None,
    contigRecoding: Map[String, String] = null,
    skipInvalidLoci: Boolean = false) {
    val hConf = hc.hadoopConf

    val statuses = LoadBgen.getAllFileStatuses(hConf, files)
    val bgenFilePaths = statuses.map(_.getPath.toString)
    val indexFilePaths = LoadBgen.getIndexFileNames(hConf, bgenFilePaths, indexFileMap)

    indexFilePaths.foreach { f =>
      assert(f.endsWith(".idx2"))
      if (hConf.exists(f))
        hConf.delete(f, recursive = true)
    }

    val recoding = Option(contigRecoding).getOrElse(Map.empty[String, String])
    val referenceGenome = rg.map(ReferenceGenome.getReference)
    referenceGenome.foreach(_.validateContigRemap(recoding))

    val headers = LoadBgen.getFileHeaders(hConf, bgenFilePaths)
    LoadBgen.checkVersionTwo(headers)

    val annotationType = +TStruct()

    val settings: BgenSettings = BgenSettings(
      0, // nSamples not used if there are no entries
      NoEntries,
      dropCols = true,
      RowFields(false, false, true, true),
      referenceGenome,
      annotationType
    )

    val typ = RVDType(settings.typ.physicalType, Array(I("file_idx"), I("locus"), I("alleles")))

    val sHadoopConfBc = hc.sc.broadcast(new SerializableHadoopConfiguration(hConf))

    val partitions: Array[Partition] = headers.zipWithIndex.map { case (f, i) =>
      IndexBgenPartition(
        f.path,
        f.compressed,
        skipInvalidLoci,
        recoding,
        f.dataStart,
        f.fileByteSize,
        i,
        sHadoopConfBc)
    }

    val rowType = typ.rowType
    val locusIdx = rowType.fieldIdx(I("locus"))
    val allelesIdx = rowType.fieldIdx(I("alleles"))
    val offsetIdx = rowType.fieldIdx(I("offset"))
    val fileIdxIdx = rowType.fieldIdx(I("file_idx"))
    val (keyType, _) = rowType.virtualType.select(Array(I("file_idx"), I("locus"), I("alleles")))
    val (indexKeyType, _) = keyType.select(Array(I("locus"), I("alleles")))

    val attributes = Map("reference_genome" -> rg.orNull,
      "contig_recoding" -> recoding,
      "skip_invalid_loci" -> skipInvalidLoci)

    val rangeBounds = files.zipWithIndex.map { case (_, i) => Interval(Row(i), Row(i), includesStart = true, includesEnd = true) }
    val partitioner = new RVDPartitioner(Array(I("file_idx")), keyType.asInstanceOf[TStruct], rangeBounds)
    val crvd = BgenRDD(hc.sc, partitions, settings, null)

    RVD.unkeyed(rowType, crvd)
      .repartition(partitioner, shuffle = true)
      .toRows
      .foreachPartition({ it =>
        val partIdx = TaskContext.get.partitionId()

        using(new IndexWriter(sHadoopConfBc.value.value, indexFilePaths(partIdx), indexKeyType, annotationType, attributes = attributes)) { iw =>
          it.foreach { r =>
            assert(r.getInt(fileIdxIdx) == partIdx)
            iw += (Row(r(locusIdx), r(allelesIdx)), r.getLong(offsetIdx), Row())
          }
        }
        info(s"Finished writing index file for ${ bgenFilePaths(partIdx) }")
      })
  }
}
