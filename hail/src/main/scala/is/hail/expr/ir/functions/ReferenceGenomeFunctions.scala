package is.hail.expr.ir.functions

import is.hail.annotations.StagedRegionValueBuilder
import is.hail.asm4s
import is.hail.asm4s._
import is.hail.expr.ir._
import is.hail.expr.types
import is.hail.expr.types._
import is.hail.expr.types.physical.{PBaseStruct, PString, PStruct}
import is.hail.expr.types.virtual._
import is.hail.utils._
import is.hail.variant.{Locus, RGBase, ReferenceGenome, VariantMethods}

class ReferenceGenomeFunctions(rg: ReferenceGenome) extends RegistryFunctions {

  def rgCode(mb: EmitMethodBuilder): Code[ReferenceGenome] = mb.getReferenceGenome(rg)

  def emitLocus(mb: EmitMethodBuilder, locus: Code[Locus]): Code[Long] = {
    val srvb = new StagedRegionValueBuilder(mb, tlocus.physicalType)
    Code(emitLocus(srvb, locus), srvb.offset)
  }

  def emitLocus(srvb: StagedRegionValueBuilder, locus: Code[Locus]): Code[Unit] = {
    val llocal = srvb.mb.newLocal[Locus]
    Code(
      llocal := locus,
      srvb.start(),
      srvb.addString(locus.invoke[String]("contig")),
      srvb.advance(),
      srvb.addInt(locus.invoke[Int]("position")),
      srvb.advance()
    )
  }

  def emitVariant(mb: EmitMethodBuilder, variant: Code[(Locus, IndexedSeq[String])]): Code[Long] = {
    val vlocal = mb.newLocal[(Locus, IndexedSeq[String])]
    val alocal = mb.newLocal[IndexedSeq[String]]
    val len = mb.newLocal[Int]
    val srvb = new StagedRegionValueBuilder(mb, tvariant.physicalType)
    val addLocus = { srvb: StagedRegionValueBuilder =>
      emitLocus(srvb, Code.checkcast[Locus](vlocal.get[java.lang.Object]("_1")))
    }
    val addAlleles = { srvb: StagedRegionValueBuilder =>
      Code(
        srvb.start(len),
        Code.whileLoop(srvb.arrayIdx < len,
          srvb.addString(alocal.invoke[Int, String]("apply", srvb.arrayIdx)),
          srvb.advance()))
    }

    Code(
      vlocal := variant,
      alocal := Code.checkcast[IndexedSeq[String]](vlocal.get[java.lang.Object]("_2")),
      len := alocal.invoke[Int]("size"),
      srvb.start(),
      srvb.addBaseStruct(types.coerce[PStruct](tlocus.fundamentalType.physicalType), addLocus),
      srvb.advance(),
      srvb.addArray(talleles.physicalType, addAlleles),
      srvb.advance(),
      srvb.offset)
  }

  def emitInterval(mb: EmitMethodBuilder, interval: Code[Interval]): Code[Long] = {
    val srvb = new StagedRegionValueBuilder(mb, tinterval.physicalType)
    Code(emitInterval(srvb, interval), srvb.offset)
  }

  def emitInterval(srvb: StagedRegionValueBuilder, interval: Code[Interval]): Code[Unit] = {
    val ilocal = srvb.mb.newLocal[Interval]
    val addLocus = { (srvb: StagedRegionValueBuilder, point: String) =>
      emitLocus(srvb, Code.checkcast[Locus](ilocal.invoke[java.lang.Object](point)))
    }

    asm4s.coerce[Unit](Code(
      ilocal := interval,
      srvb.start(),
      srvb.addBaseStruct(types.coerce[PBaseStruct](tlocus.fundamentalType.physicalType), addLocus(_, "start")),
      srvb.advance(),
      srvb.addBaseStruct(types.coerce[PBaseStruct](tlocus.fundamentalType.physicalType), addLocus(_, "end")),
      srvb.advance(),
      srvb.addBoolean(ilocal.invoke[Boolean]("includesStart")),
      srvb.advance(),
      srvb.addBoolean(ilocal.invoke[Boolean]("includesEnd")),
      srvb.advance()))
  }

  def emitLiftoverLocus(mb: EmitMethodBuilder, result: Code[(Locus, Boolean)]): Code[Long] = {
    val rlocal = mb.newLocal[(Locus, Boolean)]
    val blocal = mb.newLocal[Boolean]
    val srvb = new StagedRegionValueBuilder(mb, TTuple(tlocus, TBoolean()).physicalType)
    val addLocus = { srvb: StagedRegionValueBuilder =>
      emitLocus(srvb, Code.checkcast[Locus](rlocal.get[java.lang.Object]("_1")))
    }

    Code(
      rlocal := result,
      blocal := Code.checkcast[java.lang.Boolean](rlocal.get[java.lang.Object]("_2")).invoke[Boolean]("booleanValue"),
      srvb.start(),
      srvb.addBaseStruct(types.coerce[PStruct](tlocus.fundamentalType.physicalType), addLocus),
      srvb.advance(),
      srvb.addBoolean(blocal),
      srvb.advance(),
      srvb.offset)
  }

  def emitLiftoverLocusInterval(mb: EmitMethodBuilder, result: Code[(Interval, Boolean)]): Code[Long] = {
    val rlocal = mb.newLocal[(Interval, Boolean)]
    val ilocal = mb.newLocal[Interval]
    val blocal = mb.newLocal[Boolean]
    val srvb = new StagedRegionValueBuilder(mb, TTuple(tinterval, TBoolean()).physicalType)
    val addInterval = { srvb: StagedRegionValueBuilder =>
      emitInterval(srvb, ilocal)
    }

    Code(
      rlocal := result,
      ilocal := Code.checkcast[Interval](rlocal.get[java.lang.Object]("_1")),
      blocal := Code.checkcast[java.lang.Boolean](rlocal.get[java.lang.Object]("_2")).invoke[Boolean]("booleanValue"),
      srvb.start(),
      srvb.addBaseStruct(types.coerce[PStruct](tinterval.fundamentalType.physicalType), addInterval),
      srvb.advance(),
      srvb.addBoolean(blocal),
      srvb.advance(),
      srvb.offset)
  }

  var registered: Set[String] = Set[String]()

  val tlocus = TLocus(rg)
  val talleles = TArray(TString())
  val tvariant = TStruct(I("locus") -> tlocus, I("alleles") -> talleles)
  val tinterval = TInterval(tlocus)

  def removeRegisteredFunctions(): Unit =
    registered.foreach(IRFunctionRegistry.removeIRFunction)

  def registerRGCode(
    mname: String, args: Array[Type], rt: Type)(
    impl: (EmitMethodBuilder, Array[Code[_]]) => Code[_]
  ): Unit = {
    val newName = rg.wrapFunctionName(mname)
    registered += newName
    registerCode(newName, args, rt)(impl)
  }

  def registerRGCode[A1](
    mname: String, arg1: Type, rt: Type)(
    impl: (EmitMethodBuilder, Code[A1]) => Code[_]
  ): Unit =
    registerRGCode(mname, Array[Type](arg1), rt) {
      case (mb, Array(a1: Code[A1] @unchecked)) => impl(mb, a1)
    }

  def registerRGCode[A1, A2](
    mname: String, arg1: Type, arg2: Type, rt: Type)(
    impl: (EmitMethodBuilder, Code[A1], Code[A2]) => Code[_]
  ): Unit =
    registerRGCode(mname, Array[Type](arg1, arg2), rt) {
      case (mb, Array(a1: Code[A1] @unchecked, a2: Code[A2] @unchecked)) => impl(mb, a1, a2)
    }

  def registerRGCode[A1, A2, A3, A4](
    mname: String, arg1: Type, arg2: Type, arg3: Type, arg4: Type, rt: Type)(
    impl: (EmitMethodBuilder, Code[A1], Code[A2], Code[A3], Code[A4]) => Code[_]
  ): Unit =
    registerRGCode(mname, Array[Type](arg1, arg2, arg3, arg4), rt) {
      case (mb, Array(
      a1: Code[A1] @unchecked,
      a2: Code[A2] @unchecked,
      a3: Code[A3] @unchecked,
      a4: Code[A4] @unchecked)) => impl(mb, a1, a2, a3, a4)
    }

  def registerRGCode[A1, A2, A3, A4, A5](
    mname: String, arg1: Type, arg2: Type, arg3: Type, arg4: Type, arg5: Type, rt: Type)(
    impl: (EmitMethodBuilder, Code[A1], Code[A2], Code[A3], Code[A4], Code[A5]) => Code[_]
  ): Unit =
    registerRGCode(mname, Array[Type](arg1, arg2, arg3, arg4, arg5), rt) {
      case (mb, Array(
      a1: Code[A1] @unchecked,
      a2: Code[A2] @unchecked,
      a3: Code[A3] @unchecked,
      a4: Code[A4] @unchecked,
      a5: Code[A5] @unchecked)) => impl(mb, a1, a2, a3, a4, a5)
    }

  def registerAll() {

    val locusClass = Locus.getClass

    registerRGCode("Locus", TString(), TLocus(rg)) {
      (mb, locusoff: Code[Long]) =>
        val slocus = asm4s.coerce[String](wrapArg(mb, TString())(locusoff))
        val locus = Code
          .invokeScalaObject[String, RGBase, Locus](
          locusClass, "parse", slocus, rgCode(mb))
        emitLocus(mb, locus)
    }

    registerRGCode("Locus", TString(), TInt32(), TLocus(rg)) {
      (mb, contig: Code[Long], pos: Code[Int]) =>
        val srvb = new StagedRegionValueBuilder(mb, tlocus.physicalType)
        val scontig = asm4s.coerce[String](wrapArg(mb, TString())(contig))
        Code(
          rgCode(mb).invoke[String, Int, Unit]("checkLocus", scontig, pos),
          srvb.start(),
          srvb.addIRIntermediate(PString())(contig),
          srvb.advance(),
          srvb.addInt(pos),
          srvb.offset)
    }

    registerRGCode("LocusAlleles", TString(), tvariant) {
      (mb, variantoff: Code[Long]) =>
        val svar = asm4s.coerce[String](wrapArg(mb, TString())(variantoff))
        val variant = Code
          .invokeScalaObject[String, RGBase, (Locus, IndexedSeq[String])](
          VariantMethods.getClass, "parse", svar, rgCode(mb))
        emitVariant(mb, variant)
    }

    registerRGCode("LocusInterval", TString(), tinterval) {
      (mb, ioff: Code[Long]) =>
        val sinterval = asm4s.coerce[String](wrapArg(mb, TString())(ioff))
        val interval = Code
          .invokeScalaObject[String, RGBase, Interval](
          locusClass, "parseInterval", sinterval, rgCode(mb))
        emitInterval(mb, interval)
    }

    registerRGCode("LocusInterval", TString(), TInt32(), TInt32(), TBoolean(), TBoolean(), tinterval) {
      (mb, locoff: Code[Long], pos1: Code[Int], pos2: Code[Int], include1: Code[Boolean], include2: Code[Boolean]) =>
        val sloc = asm4s.coerce[String](wrapArg(mb, TString())(locoff))
        val interval = Code
          .invokeScalaObject[String, Int, Int, Boolean, Boolean, RGBase, Interval](
          locusClass, "makeInterval", sloc, pos1, pos2, include1, include2, rgCode(mb))
        emitInterval(mb, interval)
    }

    registerRGCode("isValidContig", TString(), TBoolean()) {
      (mb, contig: Code[Long]) =>
        val scontig = asm4s.coerce[String](wrapArg(mb, TString())(contig))
        rgCode(mb).invoke[String, Boolean]("isValidContig", scontig)
    }

    registerRGCode("isValidLocus", TString(), TInt32(), TBoolean()) {
      (mb, contig: Code[Long], pos: Code[Int]) =>
        val scontig = asm4s.coerce[String](wrapArg(mb, TString())(contig))
        rgCode(mb).invoke[String, Int, Boolean]("isValidLocus", scontig, pos)
    }

    registerRGCode("getReferenceSequenceFromValidLocus", TString(), TInt32(), TInt32(), TInt32(), TString()) {
      (mb, contig: Code[Long], pos: Code[Int], before: Code[Int], after: Code[Int]) =>
        val scontig = asm4s.coerce[String](wrapArg(mb, TString())(contig))
        unwrapReturn(mb, TString())(rgCode(mb).invoke[String, Int, Int, Int, String]("getSequence", scontig, pos, before, after))
    }

    registerIR(rg.wrapFunctionName("getReferenceSequence"), TString(), TInt32(), TInt32(), TInt32()) {
      (contig, pos, before, after) =>
        val getRef = IRFunctionRegistry.lookupConversion(
          rg.wrapFunctionName("getReferenceSequenceFromValidLocus"),
          Seq(TString(), TInt32(), TInt32(), TInt32())).get
        val isValid = IRFunctionRegistry.lookupConversion(
          rg.wrapFunctionName("isValidLocus"),
          Seq(TString(), TInt32())).get
        If(isValid(Array(contig, pos)), getRef(Array(contig, pos, before, after)), NA(TString()))
    }

    registerRGCode("globalPosToLocus", TInt64(), TLocus(rg)) {
      case (mb, globalPosition: Code[Long]) =>
        val locus = rgCode(mb).invoke[Long, Locus]("globalPosToLocus", globalPosition)
        emitLocus(mb, locus)
    }

    registerRGCode("locusToGlobalPos", TLocus(rg), TInt64()) {
      case (mb, locus: Code[Long]) =>
        val locusObject = Code.checkcast[Locus](wrapArg(mb, tlocus)(locus).asInstanceOf[Code[AnyRef]])
        unwrapReturn(mb, TInt64())(rgCode(mb).invoke[Locus, Long]("locusToGlobalPos", locusObject))
    }
  }
}

class LiftoverFunctions(rg: ReferenceGenome, destRG: ReferenceGenome) extends ReferenceGenomeFunctions(rg) {

  def registerLiftoverCode(
    mname: String, args: Array[Type], rt: Type)(
    impl: (EmitMethodBuilder, Array[EmitTriplet]) => EmitTriplet
  ): Unit = {
    val newName = destRG.wrapFunctionName(rg.wrapFunctionName(mname))
    registered += newName
    registerCodeWithMissingness(newName, args, rt)(impl)
  }

  def registerLiftoverCode(
    mname: String, arg1: Type, arg2: Type, rt: Type)(
    impl: (EmitMethodBuilder, EmitTriplet, EmitTriplet) => EmitTriplet
  ): Unit =
    registerLiftoverCode(mname, Array[Type](arg1, arg2), rt) {
      case (mb, Array(a1, a2)) => impl(mb, a1, a2)
    }

  override def registerAll() {

    registerLiftoverCode("liftoverLocus", tlocus, TFloat64(), TStruct(Identifier("result") -> TLocus(destRG), I("is_negative_strand") -> TBoolean())) {
      (mb, loc, minMatch) =>
        val locus = Code.checkcast[Locus](asm4s.coerce[AnyRef](wrapArg(mb, TLocus(rg))(loc.value[Long])))
        val tlocal = mb.newLocal[(Locus, Boolean)]
        val lifted = rgCode(mb).invoke[String, Locus, Double, (Locus, Boolean)]("liftoverLocus", destRG.name, locus, minMatch.value[Double])

        EmitTriplet(
          Code(loc.setup, minMatch.setup, tlocal := Code._null),
          loc.m || minMatch.m || Code(tlocal := lifted, tlocal.isNull),
          emitLiftoverLocus(mb, tlocal)
        )
    }

    registerLiftoverCode("liftoverLocusInterval", tinterval, TFloat64(), TStruct(I("result") -> TInterval(TLocus(destRG)), I("is_negative_strand") -> TBoolean())) {
      (mb, i, minMatch) =>
        val interval = Code.checkcast[Interval](asm4s.coerce[AnyRef](wrapArg(mb, tinterval)(i.value[Long])))
        val tlocal = mb.newLocal[(Interval, Boolean)]
        val lifted = rgCode(mb).invoke[String, Interval, Double, (Interval, Boolean)]("liftoverLocusInterval", destRG.name, interval, minMatch.value[Double])

        EmitTriplet(
          Code(i.setup, minMatch.setup, tlocal := Code._null),
          i.m || minMatch.m || Code(tlocal := lifted, tlocal.isNull),
          emitLiftoverLocusInterval(mb, tlocal)
        )
    }
  }
}
