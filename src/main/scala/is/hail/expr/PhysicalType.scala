package is.hail.expr

sealed abstract class PhysicalType {
  def virtualType: Type

  def required: Boolean

  def byteSize: Long

  def alignment: Long
}

case class PBoolean(required: Boolean) extends PhysicalType {
  val virtualType: Type = TBoolean

  def byteSize: Long = 1

  def alignment: Long = 1
}

case class PInt32(required: Boolean) extends PhysicalType {
  val virtualType: Type = TInt32

  def byteSize: Long = 4

  def alignment: Long = 4
}

case class PInt64(required: Boolean) extends PhysicalType {
  val virtualType: Type = TInt64

  def byteSize: Long = 8

  def alignment: Long = 8
}

case class PFloat32(required: Boolean) extends PhysicalType {
  val virtualType: Type = TFloat32

  def byteSize: Long = 4

  def alignment: Long = 4
}

case class PFloat64(required: Boolean) extends PhysicalType {
  val virtualType: Type = TFloat64

  def byteSize: Long = 8

  def alignment: Long = 8
}

abstract class PNaturalContainer extends PhysicalType {
  def elementPType: PhysicalType


}
