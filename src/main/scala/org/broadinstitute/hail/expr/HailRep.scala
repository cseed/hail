package org.broadinstitute.hail.expr

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.broadinstitute.hail.utils.Interval
import org.broadinstitute.hail.variant.{AltAllele, Genotype, Locus, Variant}
import spire.std.OptionEq

trait HailRep[T] {
  def typ: Type
}

object HailRep {

  implicit object boolHr extends HailRep[Boolean] {
    def typ = TBoolean
  }

  implicit object intHr extends HailRep[Int] {
    def typ = TInt
  }

  implicit object longHr extends HailRep[Long] {
    def typ = TLong
  }

  implicit object floatHr extends HailRep[Float] {
    def typ = TFloat
  }

  implicit object doubleHr extends HailRep[Double] {
    def typ = TDouble
  }

  implicit object stringHr extends HailRep[String] {
    def typ = TString
  }

  // not implicit to make stringHr the default
  object sampleHr extends HailRep[String] {
    def typ = TSample
  }

  implicit object genotypeHr extends HailRep[Genotype] {
    def typ = TGenotype
  }

  implicit object variantHr extends HailRep[Variant] {
    def typ = TVariant
  }

  implicit object locusHr extends HailRep[Locus] {
    def typ = TLocus
  }

  implicit object altAlleleHr extends HailRep[AltAllele] {
    def typ = TAltAllele
  }

  implicit object locusIntervalHr extends HailRep[Interval[Locus]] {
    def typ = TInterval
  }

  implicit def arrayHr[T](implicit hrt: HailRep[T]) = new HailRep[IndexedSeq[T]] {
    def typ = TArray(hrt.typ)
  }

  implicit def setHr[T](implicit hrt: HailRep[T]) = new HailRep[Set[T]] {
    def typ = TSet(hrt.typ)
  }

  implicit def dictHr[T](implicit hrt: HailRep[T]) = new HailRep[Map[String, T]] {
    def typ = TDict(hrt.typ)
  }

  implicit def unaryHr[T, U](implicit hrt: HailRep[T], hru: HailRep[U]) = new HailRep[(Any) => Any] {
    def typ = TFunction(Seq(hrt.typ), hru.typ)
  }
}