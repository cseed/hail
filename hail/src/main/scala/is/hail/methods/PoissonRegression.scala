package is.hail.methods

import breeze.linalg._
import is.hail.annotations._
import is.hail.expr.ir.{IRParser, Sym, TableLiteral, TableValue}
import is.hail.expr.types.virtual.{TFloat64, TStruct}
import is.hail.expr.types.TableType
import is.hail.stats._
import is.hail.table.Table
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

object PoissonRegression {

  def apply(vsm: MatrixTable,
    test: String,
    yField: String,
    xField: String,
    covFields: java.util.ArrayList[String],
    passThrough: java.util.ArrayList[String]): Table =
    PoissonRegression(vsm, test,
      IRParser.parseSymbol(yField),
      IRParser.parseSymbol(xField),
      covFields.asScala.map(IRParser.parseSymbol).toArray,
      passThrough.asScala.map(IRParser.parseSymbol).toArray)

    def apply(vsm: MatrixTable,
      test: String,
      yField: Sym,
      xField: Sym,
      covFields: Array[Sym],
      passThrough: Array[Sym]): Table = {
        val poisRegTest = PoissonRegressionTest.tests(test)

    val (y, cov, completeColIdx) = RegressionUtils.getPhenoCovCompleteSamples(vsm, yField, covFields)

    if (!y.forall(yi => math.floor(yi) == yi && yi >= 0))
      fatal(s"For poisson regression, y must be numeric with all values non-negative integers")
    if (sum(y) == 0)
      fatal(s"For poisson regression, y must have at least one non-zero value")

    val n = y.size
    val k = cov.cols
    val d = n - k - 1

    if (d < 1)
      fatal(s"$n samples and ${ k + 1 } ${ plural(k, "covariate") } (including x) implies $d degrees of freedom.")

    info(s"poisson_regression_rows: running $test on $n samples for response variable y,\n"
      + s"    with input variable x, and ${ k } additional ${ plural(k, "covariate") }...")

    val nullModel = new PoissonRegressionModel(cov, y)
    var nullFit = nullModel.fit()

    if (!nullFit.converged)
      fatal("Failed to fit poisson regression null model (standard MLE with covariates only): " + (
        if (nullFit.exploded)
          s"exploded at Newton iteration ${ nullFit.nIter }"
        else
          "Newton iteration failed to converge"))

    val sc = vsm.sparkContext
    val completeColIdxBc = sc.broadcast(completeColIdx)

    val yBc = sc.broadcast(y)
    val XBc = sc.broadcast(new DenseMatrix[Double](n, k + 1, cov.toArray ++ Array.ofDim[Double](n)))
    val nullFitBc = sc.broadcast(nullFit)
    val poisRegTestBc = sc.broadcast(poisRegTest)

    val fullRowType = vsm.rvRowType.physicalType
    val entryArrayType = vsm.matrixType.entryArrayType.physicalType
    val entryType = vsm.entryType.physicalType
    val fieldType = entryType.field(xField).typ

    assert(fieldType.virtualType.isOfType(TFloat64()))

    val entryArrayIdx = vsm.entriesIndex
    val fieldIdx = entryType.fieldIdx(xField)

    val passThroughType = TStruct(passThrough.map(f => f -> vsm.rowType.field(f).typ): _*)
    val tableType = TableType(vsm.rowKeyStruct ++ passThroughType ++ poisRegTest.schema, vsm.rowKey, TStruct())
    val newRVDType = tableType.canonicalRVDType
    val copiedFieldIndices = (vsm.rowKey ++ passThrough).map(vsm.rvRowType.fieldIdx(_)).toArray

    val newRVD = vsm.rvd.mapPartitions(newRVDType) { it =>
      val rvb = new RegionValueBuilder()
      val rv2 = RegionValue()

      val missingCompleteCols = new ArrayBuilder[Int]()

      val X = XBc.value.copy
      it.map { rv =>
        RegressionUtils.setMeanImputedDoubles(X.data, n * k, completeColIdxBc.value, missingCompleteCols,
          rv, fullRowType, entryArrayType, entryType, entryArrayIdx, fieldIdx)

        rvb.set(rv.region)
        rvb.start(newRVDType.rowType)
        rvb.startStruct()
        rvb.addFields(fullRowType, rv, copiedFieldIndices)
        poisRegTestBc.value
          .test(X, yBc.value, nullFitBc.value, "poisson")
          .addToRVB(rvb)
        rvb.endStruct()

        rv2.set(rv.region, rvb.end())
        rv2
      }
    }.persist(StorageLevel.MEMORY_AND_DISK)
    new Table(vsm.hc, TableLiteral(TableValue(tableType, BroadcastRow.empty(sc), newRVD)))
  }
}
