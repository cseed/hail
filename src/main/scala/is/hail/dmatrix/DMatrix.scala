package is.hail.dmatrix

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.language.{existentials, implicitConversions}

object DMatrix {
  final val fileVersion: Int = 0x101

  def read(hc: HailContext, dirname: String,
    dropCols: Boolean = false, dropRows: Boolean = false): DMatrix = {
    ???
  }
}

case class Region(mem: Array[Long]) // aka MemoryBuffer

case class Value(r: Region, var offset: Long)

case class DMatrixValue(
  global: Value,
  // FIXME should this be RDD[Value]?
  cols: Value,
  rows: DMatrixRowRDD) {
}

case class DMatrixType(
  rowPartitionKey: Array[String],
  // superset of rowPartitionKey
  rowKey: Array[String],
  rowAnnotation: Array[String],
  colKey: Array[String],
  colAnnotation: Array[String],
  typ: TStruct)

abstract class DMatrixAST {
  def typ: DMatrixType

  def execute(hc: HailContext): DMatrixValue
}

class DMatrix(val hc: HailContext,
  val ast: DMatrixAST) {

  // FIXME private?
  def sc: SparkContext = hc.sc

  def typ: DMatrixType = ast.typ

  lazy val value: DMatrixValue = {
    // FIXME optimize
    ast.execute(hc)
  }

  lazy val DMatrixValue(global, cols, rows) = value

  lazy val colsBc: Broadcast[Annotation] = sc.broadcast(cols)

  def write(dirname: String, overwrite: Boolean = false) {
    ???
  }
}
