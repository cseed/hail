package is.hail.methods

import is.hail.expr._
import is.hail.utils._
import is.hail.variant.{GenomeReference, Locus, VariantSampleMatrix}

object ImputeSexPlink {
  def apply(in: VariantSampleMatrix,
    mafThreshold: Double,
    includePar: Boolean,
    fMaleThreshold: Double,
    fFemaleThreshold: Double,
    popFrequencyExpr: Option[String]): VariantSampleMatrix = {
    var vsm = in

    val gr = vsm.vSignature match {
      case TVariant(x, _) => x.asInstanceOf[GenomeReference]
    }

    vsm.typecheck()

    val xIntervals = IntervalTree(gr.xContigs.map(contig => Interval(Locus(contig, 0), Locus(contig, gr.contigLength(contig)))).toArray)
    vsm = vsm.filterIntervals(xIntervals, keep = true)

    vsm.typecheck()

    if (!includePar)
      vsm = vsm.filterIntervals(IntervalTree(gr.par), keep = false)

    vsm.typecheck()

    vsm = vsm.annotateVariantsExpr(
      s"va = ${ popFrequencyExpr.getOrElse("gs.map(g => g.GT.nNonRefAlleles).sum() / gs.filter(g => g.GT.isCalled).count() / 2") }")

    vsm.typecheck()

    val resultSA = vsm
      .filterVariantsExpr(s"va > $mafThreshold")
      .annotateSamplesExpr(s"""sa =
let ib = gs.map(g => g.GT).inbreeding(g => va) and
    isFemale = if (ib.Fstat < $fFemaleThreshold) true else if (ib.Fstat > $fMaleThreshold) false else NA: Boolean
 in merge({ isFemale: isFemale }, ib)""")
      .samplesKT()

    in.annotateSamplesTable(resultSA, root = "sa.imputesex")
  }
}
