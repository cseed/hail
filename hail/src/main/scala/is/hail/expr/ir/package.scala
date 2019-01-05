package is.hail.expr

import is.hail.asm4s
import is.hail.asm4s._
import is.hail.expr.ir.functions.IRFunctionRegistry
import is.hail.expr.types.physical.PType
import is.hail.expr.types.virtual._
import is.hail.utils._

import scala.language.implicitConversions

package object ir {
  type TokenIterator = BufferedIterator[Token]

  def genSym(base: String): Sym = Sym.gen(base)

  implicit def stringToSym(s: String): Sym = Identifier(s)

  implicit def iseqStringToISeqSym(a: IndexedSeq[String]): IndexedSeq[Sym] = a.map(stringToSym)

  implicit def iseqSym[K](a: IndexedSeq[(String, K)]): IndexedSeq[(Sym, K)] = a.map { case (k, v) => (stringToSym(k), v) }

  implicit def seqSym[K](a: Seq[(String, K)]): Seq[(Sym, K)] = a.map { case (k, v) => (stringToSym(k), v) }

  implicit def mapSym[K](m: Map[String, K]): Map[Sym, K] = m.map { case (k, v) => (stringToSym(k), v) }

  def NamedIRSeq(args: (Any, IR)*): Seq[(Sym, IR)] = args.map { case (k, v) => (toSym(k), v) }

  def SymTypeMap(args: (Any, Type)*): Map[Sym, Type] = args.map { case (k, v) => (toSym(k), v) }.toMap

  def toSym(s: Any): Sym = s match {
    case s: Sym => s
    case s: String => Identifier(s)
  }

  def typeToTypeInfo(t: PType): TypeInfo[_] = typeToTypeInfo(t.virtualType)

  def typeToTypeInfo(t: Type): TypeInfo[_] = t.fundamentalType match {
    case _: TInt32 => typeInfo[Int]
    case _: TInt64 => typeInfo[Long]
    case _: TFloat32 => typeInfo[Float]
    case _: TFloat64 => typeInfo[Double]
    case _: TBoolean => typeInfo[Boolean]
    case _: TBinary => typeInfo[Long]
    case _: TArray => typeInfo[Long]
    case _: TBaseStruct => typeInfo[Long]
    case TVoid => typeInfo[Unit]
    case _ => throw new RuntimeException(s"unsupported type found, $t")
  }

  def defaultValue(t: PType): Code[_] = defaultValue(t.virtualType)

  def defaultValue(t: Type): Code[_] = typeToTypeInfo(t) match {
    case UnitInfo => Code._empty[Unit]
    case BooleanInfo => false
    case IntInfo => 0
    case LongInfo => 0L
    case FloatInfo => 0.0f
    case DoubleInfo => 0.0
    case ti => throw new RuntimeException(s"unsupported type found: $t whose type info is $ti")
  }

  // Build consistent expression for a filter-condition with keep polarity,
  // using Let to manage missing-ness.
  def filterPredicateWithKeep(irPred: ir.IR, keep: Boolean): ir.IR = {
    val pred = genSym("pred")
    ir.Let(pred,
      if (keep) irPred else ir.ApplyUnaryPrimOp(ir.Bang(), irPred),
      ir.If(ir.IsNA(ir.Ref(pred, TBoolean())),
        ir.False(),
        ir.Ref(pred, TBoolean())))
  }

  private[ir] def coerce[T](c: Code[_]): Code[T] = asm4s.coerce(c)

  private[ir] def coerce[T](lr: Settable[_]): Settable[T] = lr.asInstanceOf[Settable[T]]

  private[ir] def coerce[T](ti: TypeInfo[_]): TypeInfo[T] = ti.asInstanceOf[TypeInfo[T]]

  private[ir] def coerce[T <: Type](x: Type): T = types.coerce[T](x)

  private[ir] def coerce[T <: PType](x: PType): T = types.coerce[T](x)

  def invoke(name: String, args: IR*): IR = IRFunctionRegistry.lookupConversion(name, args.map(_.typ)) match {
    case Some(f) => f(args)
    case None => fatal(s"no conversion found for $name(${args.map(_.typ).mkString(", ")})")
  }


  implicit def irToPrimitiveIR(ir: IR): PrimitiveIR = new PrimitiveIR(ir)

  implicit def intToIR(i: Int): IR = I32(i)
  implicit def longToIR(l: Long): IR = I64(l)
  implicit def floatToIR(f: Float): IR = F32(f)
  implicit def doubleToIR(d: Double): IR = F64(d)
  implicit def booleanToIR(b: Boolean): IR = if (b) True() else False()
}
