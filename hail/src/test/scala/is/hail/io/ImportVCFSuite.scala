package is.hail.io

import htsjdk.tribble.readers.TabixReader
import is.hail.check.Prop._
import is.hail.SparkSuite
import is.hail.io.vcf.ExportVCF
import is.hail.variant._
import org.testng.annotations.Test

import scala.collection.JavaConverters._

class ImportVCFSuite extends SparkSuite {
  @Test def foo() {
    // adds .tbi suffix
    val r = new TabixReader("/Users/cotton/sample.vcf.gz")
    val chroms = r.getChromosomes.asScala
    println(chroms)

    val x = r.parseReg("20:16729654-17417513")
    println(x.toIndexedSeq)

    // [a, b] => a-1, b
    val it = r.query("20", 16729653, 17417513)

    var l = it.next()
    while (l != null) {
      println(l.take(20) + "...")
      l = it.next()
    }
  }

  @Test def randomExportImportIsIdentity() {
    forAll(MatrixTable.gen(hc, VSMSubgen.random)) { vds =>

      val truth = {
        val f = tmpDir.createTempFile(extension="vcf")
        ExportVCF(vds, f)
        hc.importVCF(f, rg = Some(vds.referenceGenome))
      }

      val actual = {
        val f = tmpDir.createTempFile(extension="vcf")
        ExportVCF(truth, f)
        hc.importVCF(f, rg = Some(vds.referenceGenome))
      }

      truth.same(actual)
    }.check()
  }
}
