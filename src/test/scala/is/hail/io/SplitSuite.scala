package is.hail.io

import is.hail.SparkSuite
import is.hail.check.Prop._
import is.hail.check.{Gen, Properties}
import is.hail.utils._
import is.hail.variant.{AltAllele, VSMSubgen, Variant, VariantSampleMatrix}
import org.testng.annotations.Test

class SplitSuite extends SparkSuite {

  object Spec extends Properties("MultiSplit") {
    val splittableVariantGen = for {
      contig <- Gen.const("1")
      start <- Gen.choose(1, 100)
      motif <- Gen.oneOf("AT", "AC", "CT", "GA", "GT", "CCA", "CAT", "CCT")
      ref <- Gen.choose(1, 10).map(motif * _)
      alts <- Gen.distinctBuildableOf[Array, AltAllele](Gen.choose(1, 10).map(motif * _).filter(_ != ref).map(a => AltAllele(ref, a)))
    } yield Variant(contig, start, ref, alts)

    property("splitMulti maintains variants") = forAll(VariantSampleMatrix.gen(hc,
      VSMSubgen.random.copy(vGen = _ => splittableVariantGen))) { vds =>
      val method1 = vds.splitMulti().variants.collect().toSet
      val method2 = vds.variants.flatMap { v1 =>
        val v = v1.asInstanceOf[Variant]
        v.altAlleles.iterator
          .map { aa =>
            Variant(v.contig, v.start, v.ref, Array(aa)).minRep
          }
      }.collect().toSet

      method1 == method2
    }
  }

  @Test def splitTest() {
    Spec.check()

    val vds1 = hc.importVCF("src/test/resources/split_test.vcf")
      .splitMulti()

    val vds2 = hc.importVCF("src/test/resources/split_test_b.vcf")

    // test splitting and downcoding
    vds1.mapWithKeys((v, s, g) => ((v, s), g))
      .join(vds2.mapWithKeys((v, s, g) => ((v, s), g)))
      .foreach { case (k, (g1, g2)) =>
        if (g1 != g2)
          println(s"$g1, $g2")
        simpleAssert(g1 == g2)
      }

    val wasSplitQuerier = vds1.vaSignature.query("wasSplit")

    // test for wasSplit
    vds1.mapWithAll((v, va, s, sa, g) => (v.asInstanceOf[Variant].start, wasSplitQuerier(va).asInstanceOf[Boolean]))
      .foreach { case (i, b) =>
        simpleAssert(b == (i != 1180))
      }
  }
}
