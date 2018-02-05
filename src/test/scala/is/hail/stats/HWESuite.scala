package is.hail.stats

import is.hail.SparkSuite
import is.hail.check._
import is.hail.methods.{SplitMulti, VariantQC}
import is.hail.utils._
import is.hail.testUtils._
import is.hail.variant._
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class HWESuite extends SparkSuite {

  @Test def test() {
    val a = VariantQC(hc.importVCF("src/test/resources/HWE_test.vcf"))
      .variantsKT()
      .query(Array("locus.map(_ => locus.position).collect()",
        "qc.map(qc => {r: qc.rExpectedHetFrequency, p: qc.pHWE}).collect()"))
      .map(_._1.asInstanceOf[IndexedSeq[Any]])

    val r = a(0).zip(a(1)).toMap


    assert(r(1) == Row(0.0, 0.5))
    assert(r(2) == Row(0.25, 0.5))
    assert(D_==(r(3).asInstanceOf[Row].getAs[Double](1), LeveneHaldane(4, 3).exactMidP(1)))
    assert(D_==(r(4).asInstanceOf[Row].getAs[Double](1), LeveneHaldane(4, 4).exactMidP(2)))
    assert(D_==(r(5).asInstanceOf[Row].getAs[Double](1), LeveneHaldane(3, 1).exactMidP(1)))
    assert(r(6) == Row(null, 0.5))
  }

  @Test def testExpr() {
    val p = Prop.forAll(MatrixTable.gen(hc, VSMSubgen.random)) { vds: MatrixTable =>
      var vds2 = SplitMulti(vds)
      vds2 = VariantQC(vds2)
      vds2 = vds2
        .annotateVariantsExpr("hweExpr = hwe(va.qc.nHomRef, va.qc.nHet, va.qc.nHomVar)")
        .annotateVariantsExpr("hweAgg = gs.map(g => g.GT).hardyWeinberg()")

      val (_, q1) = vds2.queryVA("va.qc.rExpectedHetFrequency")
      val (_, q2) = vds2.queryVA("va.qc.pHWE")

      val (_, q1e) = vds2.queryVA("va.hweExpr.rExpectedHetFrequency")
      val (_, q2e) = vds2.queryVA("va.hweExpr.pHWE")


      val (_, q1a) = vds2.queryVA("va.hweExpr.rExpectedHetFrequency")
      val (_, q2a) = vds2.queryVA("va.hweExpr.pHWE")

      vds2
        .variantsAndAnnotations
        .forall { case (v, va) => q1(va) == q1e(va) && q1(va) == q1a(va) &&
          q2(va) == q2e(va) && q2(va) == q2a(va)
        }
    }
    p.check()
  }
}
