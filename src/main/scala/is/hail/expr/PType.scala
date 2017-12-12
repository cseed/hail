package is.hail.expr

import is.hail.annotations.{MemoryBuffer, RegionValue}
import is.hail.asm4s.Code

sealed abstract class PType extends Serializable {
  val virtualType: Type

  val byteSize: Long

  val alignment: Long

  val required: Boolean
}

sealed abstract class PBoolean extends PType {
  val virtualType: TBoolean

  val byteSize: Long = 1

  val alignment: Long = 1
}

object PBoolean {
  def apply(required: Boolean): PBoolean =
    if (required) PBooleanRequired else PBooleanOptional
}

case object PBooleanRequired extends PBoolean {
  val virtualType: TBoolean = TBooleanRequired

  val required: Boolean = true
}

case object PBooleanOptional extends PBoolean {
  val virtualType: TBoolean = TBooleanOptional

  val required: Boolean = false
}

sealed abstract class PInt32 extends PType {
  val virtualType: TInt32

  val byteSize: Long = 4

  val alignment: Long = 4
}

object PInt32 {
  def apply(required: Boolean): PInt32 =
    if (required) PInt32Required else PInt32Optional
}

case object PInt32Required extends PInt32 {
  val virtualType: TInt32 = TInt32Required

  val required: Boolean = true
}

case object PInt32Optional extends PInt32 {
  val virtualType: TInt32 = TInt32Optional

  val required: Boolean = false
}

sealed abstract class PInt64 extends PType {
  val virtualType: TInt64

  val byteSize: Long = 8

  val alignment: Long = 8
}

object PInt64 {
  def apply(required: Boolean): PInt64 =
    if (required) PInt64Required else PInt64Optional
}

case object PInt64Required extends PInt64 {
  val virtualType: TInt64 = TInt64Required

  val required: Boolean = true
}

case object PInt64Optional extends PInt64 {
  val virtualType: TInt64 = TInt64Optional

  val required: Boolean = false
}

sealed abstract class PFloat32 extends PType {
  val virtualType: TFloat32

  val byteSize: Long = 4

  val alignment: Long = 4
}

object PFloat32 {
  def apply(required: Boolean): PFloat32 =
    if (required) PFloat32Required else PFloat32Optional
}

case object PFloat32Required extends PFloat32 {
  val virtualType: TFloat32 = TFloat32Required

  val required: Boolean = true
}

case object PFloat32Optional extends PFloat32 {
  val virtualType: TFloat32 = TFloat32Optional

  val required: Boolean = false
}

sealed abstract class PFloat64 extends PType {
  val virtualType: TFloat64

  val byteSize: Long = 8

  val alignment: Long = 8
}

object PFloat64 {
  def apply(required: Boolean): PFloat64 =
    if (required) PFloat64Required else PFloat64Optional
}

case object PFloat64Required extends PFloat64 {
  val virtualType: TFloat64 = TFloat64Required

  val required: Boolean = true
}

case object PFloat64Optional extends PFloat64 {
  val virtualType: TFloat64 = TFloat64Optional

  val required: Boolean = false
}

sealed abstract class PBinary extends PType {
  val virtualType: TBinary

  val byteSize: Long = 8

  val alignment: Long = 8
}

object PBinary {
  def apply(required: Boolean): PBinary =
    if (required) PBinaryRequired else PBinaryOptional
}

case object PBinaryRequired extends PBinary {
  val virtualType: TBinary = TBinaryRequired

  val required: Boolean = true
}

case object PBinaryOptional extends PBinary {
  val virtualType: TBinary = TBinaryOptional

  val required: Boolean = false
}

sealed abstract class PString extends PType {
  val virtualType: TString

  val byteSize: Long = 8

  val alignment: Long = 8
}

object PString {
  def apply(required: Boolean): PString =
    if (required) PStringRequired else PStringOptional
}

case object PStringRequired extends PString {
  val virtualType: TString = TStringRequired

  val required: Boolean = true
}

case object PStringOptional extends PString {
  val virtualType: TString = TStringOptional

  val required: Boolean = false
}

/* class PPointer(target: PType, val required: Boolean) extends PType {
  val virtualType: Type = target.virtualType

  val byteSize: Long = 8

  val alignment: Long = 8
} */

abstract class PArray extends PType {
  def elementPTyp: PType

  def elementOffset(region: MemoryBuffer, aoff: Long, i: Int): Long

  def elementOffset(region: Code[MemoryBuffer], aoff: Code[Long], i: Code[Int]): Code[Long]

  def isElementDefined(region: MemoryBuffer, aoff: Long, i: Int): Boolean

  def isElementDefined(region: Code[MemoryBuffer], aoff: Code[Long], i: Code[Int]): Code[Boolean]

  def isElementDefined(rv: RegionValue, i: Int): Boolean = isElementDefined(rv.region, rv.offset, i)
}

case class PField(name: String, ptyp: PType)

abstract class PStruct extends PType {
  def pfields: IndexedSeq[PField]

  def fieldOffset(region: MemoryBuffer, offset: Long, fieldIdx: Int): Long

  def fieldOffset(region: MemoryBuffer, offset: Code[Long], fieldIdx: Int): Code[Long]

  def isFieldDefined(region: MemoryBuffer, offset: Long, fieldIdx: Int): Boolean

  def isFieldDefined(region: Code[MemoryBuffer], offset: Code[Long], fieldIdx: Int): Code[Boolean]

  def isFieldDefined(rv: RegionValue, fieldIdx: Int): Boolean = isFieldDefined(rv.region, rv.offset, fieldIdx)
}
