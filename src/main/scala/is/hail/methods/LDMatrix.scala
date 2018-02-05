package is.hail.methods

import breeze.linalg.{DenseMatrix => BDM, _}
import is.hail.HailContext
import is.hail.annotations.UnsafeRow
import is.hail.distributedmatrix.BlockMatrix
import is.hail.distributedmatrix.BlockMatrix.ops._
import is.hail.expr.Parser
import is.hail.expr.types._
import is.hail.stats.RegressionUtils
import is.hail.utils._
import is.hail.variant.{HardCallView, Locus, MatrixTable, RegionValueVariant, Variant}
import org.apache.hadoop.io._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, Matrix, Vectors}
import org.json4s._

object LDMatrix {
  /**
    * Computes the LD matrix for the given VDS.
    * @param vds VDS on which to compute Pearson correlation between pairs of variants.
    * @return LDMatrix.
    */
  def apply(vds : MatrixTable, optForceLocal: Option[Boolean]): LDMatrix = {
    val maxEntriesForLocalProduct = 25e6 // 5000 * 5000
    
    val nSamples = vds.numCols

    val localRVType = vds.rvRowType

    val locusIdx = localRVType.fieldIdx("locus")
    val allelesIdx = localRVType.fieldIdx("alleles")
    val filteredNormalizedHardCalls = vds.rvd.mapPartitions { it =>
      val view = HardCallView(localRVType)
      val rvv = new RegionValueVariant(localRVType)
      it.flatMap { rv =>
        rvv.setRegion(rv)
        view.setRegion(rv)
        RegressionUtils.normalizedHardCalls(view, nSamples).map(x => (rvv.variantObject(), x))
      }
    }.persist()

    implicit val variantOrd = vds.genomeReference.variantOrdering
    
    val variantsKept = filteredNormalizedHardCalls.map(_._1).collect()
    assert(variantsKept.isSorted, "ld_matrix: Array of variants is not sorted. This is a bug")
    val nVariantsKept = variantsKept.length

    val normalizedIndexedRows = filteredNormalizedHardCalls.map(_._2).zipWithIndex()
      .map{ case (values, idx) => IndexedRow(idx, Vectors.dense(values))}

    val normalizedBlockMatrix = new IndexedRowMatrix(normalizedIndexedRows, nVariantsKept, nSamples).toHailBlockMatrix()

    filteredNormalizedHardCalls.unpersist()

    info(s"Computing LD matrix with ${variantsKept.length} variants using $nSamples samples.")

    val nEntries: Long = nVariantsKept * nVariantsKept
    val nSamplesInverse = 1.0 / nSamples

    val computeProductLocally = optForceLocal.getOrElse(nEntries <= maxEntriesForLocalProduct)

    val irm: IndexedRowMatrix =
      if (computeProductLocally) {
        val localMat = normalizedBlockMatrix.toLocalMatrix()
        val product = localMat * localMat.t
        BlockMatrix.from(vds.sparkContext, product, normalizedBlockMatrix.blockSize).toIndexedRowMatrix()
      } else
        (normalizedBlockMatrix * normalizedBlockMatrix.t).toIndexedRowMatrix()

    val scaledIRM = new IndexedRowMatrix(irm.rows
      .map{case IndexedRow(idx, vec) => IndexedRow(idx, vec.map(d => d * nSamplesInverse))})

    LDMatrix(vds.hc, scaledIRM, variantsKept, nSamples, TVariant(vds.genomeReference))
  }

  def apply(vds: MatrixTable, forceLocal: Boolean = false): LDMatrix =
    apply(vds, Some(forceLocal))

  private val metadataRelativePath = "/metadata.json"
  private val matrixRelativePath = "/matrix"
  def read(hc: HailContext, uri: String): LDMatrix = {
    val hadoop = hc.hadoopConf
    hadoop.mkDir(uri)

    val rdd = hc.sc.sequenceFile[LongWritable, ArrayPrimitiveWritable](uri+matrixRelativePath).map { case (lw, apw) =>
      IndexedRow(lw.get(), new DenseVector(apw.get().asInstanceOf[Array[Double]]))
    }

    val LDMatrixMetadata(variants, nSamples, vTyp) =
      hc.hadoopConf.readTextFile(uri + metadataRelativePath) { isr =>
        jackson.Serialization.read[LDMatrixMetadata](isr)
      }

    new LDMatrix(hc, new IndexedRowMatrix(rdd), variants, nSamples, Parser.parseType(vTyp).asInstanceOf[TVariant])
  }
}

/**
  *
  * @param matrix Spark IndexedRowMatrix. Entry (i, j) encodes the r value between variants i and j.
  * @param variants Array of variants indexing the rows and columns of the matrix.
  * @param nSamples Number of samples used to compute this matrix.
  */
case class LDMatrix(hc: HailContext, matrix: IndexedRowMatrix, variants: Array[Variant], nSamples: Int, vTyp: TVariant) extends ExportableMatrix {
  import LDMatrix._

  def toLocalMatrix: BDM[Double] = {
    matrix.toHailBlockMatrix().toLocalMatrix()
  }

  def write(uri: String) {
    val hadoop = matrix.rows.sparkContext.hadoopConfiguration
    hadoop.mkDir(uri)

    matrix.rows.map { case IndexedRow(i, v) => (new LongWritable(i), new ArrayPrimitiveWritable(v.toArray)) }
      .saveAsSequenceFile(uri + matrixRelativePath)

    hadoop.writeTextFile(uri + metadataRelativePath) { os =>
      jackson.Serialization.write(
        LDMatrixMetadata(variants, nSamples, vTyp.toString),
        os)
    }
  }
}

case class LDMatrixMetadata(variants: Array[Variant], nSamples: Int, vTyp: String)
