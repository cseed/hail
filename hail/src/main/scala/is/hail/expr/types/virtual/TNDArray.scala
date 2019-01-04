package is.hail.expr.types.virtual

import is.hail.annotations.ExtendedOrdering
import is.hail.expr.ir.I
import is.hail.expr.types.physical.PNDArray
import org.apache.spark.sql.Row

import scala.reflect.{ClassTag, classTag}

final case class TNDArray(elementType: Type, override val required: Boolean = false) extends ComplexType {
  val representation: Type = TStruct(
    I("flags") -> TInt64Required,           // encodes data layout
    I("shape") -> TArray(TInt64Required),   // length is ndim
    I("offset") ->  TInt64Required,         // offset into data
    I("strides") -> TArray(TInt64Required), // stride in each dim, length is 1
    I("data") -> TArray(elementType)
  )

  lazy val physicalType: PNDArray = PNDArray(elementType.physicalType, required)

  def _toPretty = s"NDArray[$elementType]"

  override def scalaClassTag: ClassTag[Row] = classTag[Row]

  def _typeCheck(a: Any): Boolean = {
    a.isInstanceOf[Row] && {
      val r = a.asInstanceOf[Row]
      val s = representation.asInstanceOf[TStruct]
      r.length == s.size && s.types.zipWithIndex.forall {
        case (t, i) => t._typeCheck(r.get(i))
      }
    }
  }

  val ordering: ExtendedOrdering = TBaseStruct.getOrdering(
    representation.asInstanceOf[TStruct].types)
}
