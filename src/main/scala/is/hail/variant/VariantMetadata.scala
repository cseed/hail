package is.hail.variant

import is.hail.annotations.Annotation
import is.hail.expr._
import is.hail.utils._

object VSMLocalValue {
  def apply(sampleIds: IndexedSeq[Annotation]): VSMLocalValue =
    VSMLocalValue(Annotation.empty,
      sampleIds,
      Annotation.emptyIndexedSeq(sampleIds.length))
}

case class VSMLocalValue(
  globalAnnotation: Annotation,
  sampleIds: IndexedSeq[Annotation],
  sampleAnnotations: IndexedSeq[Annotation]) {
  assert(sampleIds.length == sampleAnnotations.length)

  def nSamples: Int = sampleIds.length

  def dropSamples(): VSMLocalValue = VSMLocalValue(globalAnnotation,
    IndexedSeq.empty[Annotation],
    IndexedSeq.empty[Annotation])
}

case class VSMFileMetadata(
  wasSplit: Boolean,
  typ: MatrixType,
  localValue: VSMLocalValue,
  nPartitions: Int)
