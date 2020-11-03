package is.hail.variant.vsm

import is.hail.HailSuite
import is.hail.annotations.BroadcastRow
import is.hail.expr.ir
import is.hail.expr.ir.{ExecuteContext, Interpret, MatrixAnnotateRowsTable, Pass2, TableLiteral, TableRange, TableValue}
import is.hail.types._
import is.hail.types.virtual.{TInt32, TStruct}
import is.hail.rvd.RVD
import is.hail.utils.FastIndexedSeq
import org.testng.annotations.Test

class PartitioningSuite extends HailSuite {
  @Test def testShuffleOnEmptyRDD() {
    val typ = TableType(TStruct("tidx" -> TInt32), FastIndexedSeq("tidx"), TStruct.empty)
    val t = TableLiteral(TableValue(ctx,
      typ, BroadcastRow.empty(ctx), RVD.empty(typ.canonicalRVDType)))
    val rangeReader = ir.MatrixRangeReader(100, 10, Some(10))
    Pass2.executeMatrix(ctx,
      MatrixAnnotateRowsTable(
        ir.MatrixRead(rangeReader.fullMatrixType, false, false, rangeReader),
        t,
        "foo",
        product = false))
      .rvd.count()
  }

  @Test def testEmptyRDDOrderedJoin() {
    val tv = Pass2.executeTable(ctx, TableRange(100, 6))

    val nonEmptyRVD = tv.rvd
    val rvdType = nonEmptyRVD.typ
    val emptyRVD = RVD.empty(rvdType)

    ExecuteContext.scoped() { ctx =>
      emptyRVD.orderedJoin(nonEmptyRVD, "left", (_, it) => it.map(_._1), rvdType, ctx).count()
      emptyRVD.orderedJoin(nonEmptyRVD, "inner", (_, it) => it.map(_._1), rvdType, ctx).count()
      nonEmptyRVD.orderedJoin(emptyRVD, "left", (_, it) => it.map(_._1), rvdType, ctx).count()
      nonEmptyRVD.orderedJoin(emptyRVD, "inner", (_, it) => it.map(_._1), rvdType, ctx).count()
    }
  }
}
