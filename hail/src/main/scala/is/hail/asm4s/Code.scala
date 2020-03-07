package is.hail.asm4s

import java.io.PrintStream
import java.lang.reflect

import is.hail.lir
import is.hail.utils._

import org.objectweb.asm.Opcodes._
import org.objectweb.asm.Type

import scala.reflect.ClassTag

object Code {
  def void[T](v: lir.StmtX): Code[T] = {
    val L = new lir.Block()
    L.append(v)
    new Code(L, L, null)
  }

  def void[T](c: Code[_], f: (lir.ValueX) => lir.StmtX): Code[T] = {
    c.end.append(f(c.v))
    new Code(c.start, c.end, null)
  }

  def void[T](c1: Code[_], c2: Code[_], f: (lir.ValueX, lir.ValueX) => lir.StmtX): Code[T] = {
    c2.end.append(f(c1.v, c2.v))
    c1.end.append(lir.goto(c2.start))
    new Code(c1.start, c2.end, null)

  }

  def apply[T](v: lir.ValueX): Code[T] = {
    val L = new lir.Block()
    new Code(L, L, v)
  }

  def concat[T](c: Code[_]*): Code[T] = ???

  def apply[T](c: Code[_], f: (lir.ValueX) => lir.ValueX): Code[T] =
    new Code(c.start, c.end, f(c.v))

  def apply[T](c1: Code[_], c2: Code[_], f: (lir.ValueX, lir.ValueX) => lir.ValueX): Code[T] = {
    c1.end.append(lir.goto(c2.start))
    new Code(c1.start, c2.end, f(c1.v, c2.v))
  }

  def apply[T](c1: Code[_], c2: Code[_], c3: Code[_], f: (lir.ValueX, lir.ValueX, lir.ValueX) => lir.ValueX): Code[T] = {
    c1.end.append(lir.goto(c2.start))
    c2.end.append(lir.goto(c3.start))
    new Code(c1.start, c3.end, f(c1.v, c2.v, c3.v))
  }

  def sequenceValues(cs: IndexedSeq[Code[_]]): (lir.Block, lir.Block, IndexedSeq[lir.ValueX]) = {
    val start = new lir.Block()
    val end = cs.foldLeft(start) { (end, c) =>
      end.append(lir.goto(c.start))
      c.end
    }
    (start, end, cs.map(_.v))
  }

  def sequence1[T](cs: IndexedSeq[Code[Unit]], v: Code[T]): Code[T] = {
    val start = new lir.Block()
    val end = (cs :+ v).foldLeft(start) { (end, c) =>
      end.append(lir.goto(c.start))
      c.end
    }
    new Code(start, end, v.v)
  }

  def apply[T](c1: Code[Unit], c2: Code[T]): Code[T] =
    sequence1(FastIndexedSeq(c1), c2)

  def apply[T](c1: Code[Unit], c2: Code[Unit], c3: Code[T]): Code[T] =
    sequence1(FastIndexedSeq(c1, c2), c3)

  def apply[T](c1: Code[Unit], c2: Code[Unit], c3: Code[Unit], c4: Code[T]): Code[T] =
    sequence1(FastIndexedSeq(c1, c2, c3), c4)

  def apply[T](c1: Code[Unit], c2: Code[Unit], c3: Code[Unit], c4: Code[Unit], c5: Code[T]): Code[T] =
    sequence1(FastIndexedSeq(c1, c2, c3, c4), c5)

  def apply[T](c1: Code[Unit], c2: Code[Unit], c3: Code[Unit], c4: Code[Unit], c5: Code[Unit], c6: Code[T]): Code[T] =
    sequence1(FastIndexedSeq(c1, c2, c3, c4, c5), c6)

  def apply[T](c1: Code[Unit], c2: Code[Unit], c3: Code[Unit], c4: Code[Unit], c5: Code[Unit], c6: Code[Unit], c7: Code[T]): Code[T] =
    sequence1(FastIndexedSeq(c1, c2, c3, c4, c5, c6), c7)

  def apply[T](c1: Code[Unit], c2: Code[Unit], c3: Code[Unit], c4: Code[Unit], c5: Code[Unit], c6: Code[Unit], c7: Code[Unit], c8: Code[T]): Code[T] =
    sequence1(FastIndexedSeq(c1, c2, c3, c4, c5, c6, c7), c8)

  def apply[T](c1: Code[Unit], c2: Code[Unit], c3: Code[Unit], c4: Code[Unit], c5: Code[Unit], c6: Code[Unit], c7: Code[Unit], c8: Code[Unit], c9: Code[T]): Code[T] =
    sequence1(FastIndexedSeq(c1, c2, c3, c4, c5, c6, c7, c8), c9)

  def apply(cs: Seq[Code[Unit]]): Code[Unit] = {
    if (cs.isEmpty)
      Code(null: lir.ValueX)
    else {
      val fcs = cs.toFastIndexedSeq
      sequence1(fcs.init, fcs.last)
    }
  }

  def foreach[A](it: Seq[A])(f: A => Code[Unit]): Code[Unit] = Code(it.map(f))

  def newInstance[T <: AnyRef](parameterTypes: Array[Class[_]], args: Array[Code[_]])(implicit tct: ClassTag[T]): Code[T] = {
    val ti = classInfo[T]

    val L = new lir.Block()

    val linst = new lir.Local(null, "new_inst", ti)
    L.append(lir.store(linst, lir.newInstance(ti)))

    val inst = new Code(L, L, lir.load(linst))
    val ctor = inst.invokeConstructor(parameterTypes, args)

    new Code(ctor.start, ctor.end, lir.load(linst))
  }

  def newInstance[T <: AnyRef]()(implicit tct: ClassTag[T], tti: TypeInfo[T]): Code[T] =
    newInstance[T](Array[Class[_]](), Array[Code[_]]())

  def newInstance[T <: AnyRef, A1](a1: Code[A1])(implicit a1ct: ClassTag[A1],
    tct: ClassTag[T], tti: TypeInfo[T]): Code[T] =
    newInstance[T](Array[Class[_]](a1ct.runtimeClass), Array[Code[_]](a1))

  def newInstance[T <: AnyRef, A1, A2](a1: Code[A1], a2: Code[A2])(implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2],
    tct: ClassTag[T], tti: TypeInfo[T]): Code[T] =
    newInstance[T](Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass), Array[Code[_]](a1, a2))

  def newInstance[T <: AnyRef, A1, A2, A3](a1: Code[A1], a2: Code[A2], a3: Code[A3])(implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2],
    a3ct: ClassTag[A3], tct: ClassTag[T], tti: TypeInfo[T]): Code[T] =
    newInstance[T](Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass), Array[Code[_]](a1, a2, a3))

  def newInstance[T <: AnyRef, A1, A2, A3, A4](a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4]
  )(implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], tct: ClassTag[T], tti: TypeInfo[T]): Code[T] =
    newInstance[T](Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass), Array[Code[_]](a1, a2, a3, a4))

  def newInstance[T <: AnyRef, A1, A2, A3, A4, A5](a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4], a5: Code[A5]
  )(implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], a5ct: ClassTag[A5], tct: ClassTag[T], tti: TypeInfo[T]): Code[T] =
    newInstance[T](Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass, a5ct.runtimeClass), Array[Code[_]](a1, a2, a3, a4, a5))

  def newArray[T](size: Code[Int])(implicit tti: TypeInfo[T]): Code[Array[T]] =
    Code(size, lir.newArray(tti))

  def whileLoop(cond: Code[Boolean], body: Code[Unit]*): Code[Unit] = {
    val L = CodeLabel()
    Code(
      L,
      cond.mux(
        Code(
          Code(body.toFastIndexedSeq),
          L.goto),
        Code._empty))
  }

  def forLoop(init: Code[Unit], cond: Code[Boolean], increment: Code[Unit], body: Code[Unit]): Code[Unit] = {
    Code(
      init,
      Code.whileLoop(cond,
        body,
        increment
      )
    )
  }

  def invokeScalaObject[S](cls: Class[_], method: String, parameterTypes: Array[Class[_]], args: Array[Code[_]])(implicit sct: ClassTag[S]): Code[S] = {
    val m = Invokeable.lookupMethod(cls, method, parameterTypes)(sct)
    val staticObj = FieldRef("MODULE$")(ClassTag(cls), ClassTag(cls), classInfo(ClassTag(cls)))
    m.invoke(staticObj.getField(), args)
  }

  def invokeScalaObject[S](cls: Class[_], method: String)(implicit sct: ClassTag[S]): Code[S] =
    invokeScalaObject[S](cls, method, Array[Class[_]](), Array[Code[_]]())

  def invokeScalaObject[A1, S](cls: Class[_], method: String, a1: Code[A1])(implicit a1ct: ClassTag[A1], sct: ClassTag[S]): Code[S] =
    invokeScalaObject[S](cls, method, Array[Class[_]](a1ct.runtimeClass), Array[Code[_]](a1))

  def invokeScalaObject[A1, A2, S](cls: Class[_], method: String, a1: Code[A1], a2: Code[A2])(implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], sct: ClassTag[S]): Code[S] =
    invokeScalaObject[S](cls, method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass), Array(a1, a2))

  def invokeScalaObject[A1, A2, A3, S](cls: Class[_], method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3])(implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], sct: ClassTag[S]): Code[S] =
    invokeScalaObject[S](cls, method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass), Array(a1, a2, a3))

  def invokeScalaObject[A1, A2, A3, A4, S](
    cls: Class[_], method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4])(
    implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], sct: ClassTag[S]): Code[S] =
    invokeScalaObject[S](cls, method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass), Array(a1, a2, a3, a4))

  def invokeScalaObject[A1, A2, A3, A4, A5, S](
    cls: Class[_], method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4], a5: Code[A5])(
    implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], a5ct: ClassTag[A5], sct: ClassTag[S]
  ): Code[S] =
    invokeScalaObject[S](
      cls, method, Array[Class[_]](
        a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass, a5ct.runtimeClass), Array(a1, a2, a3, a4, a5))

  def invokeScalaObject[A1, A2, A3, A4, A5, A6, S](
    cls: Class[_], method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4], a5: Code[A5], a6: Code[A6])(
    implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], a5ct: ClassTag[A5], a6ct: ClassTag[A6], sct: ClassTag[S]
  ): Code[S] =
    invokeScalaObject[S](
      cls, method, Array[Class[_]](
        a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass, a5ct.runtimeClass, a6ct.runtimeClass), Array(a1, a2, a3, a4, a5, a6))

  def invokeScalaObject[A1, A2, A3, A4, A5, A6, A7, S](
    cls: Class[_], method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4], a5: Code[A5], a6: Code[A6], a7: Code[A7])(
    implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], a5ct: ClassTag[A5], a6ct: ClassTag[A6], a7ct: ClassTag[A7], sct: ClassTag[S]
  ): Code[S] =
    invokeScalaObject[S](
      cls, method, Array[Class[_]](
        a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass, a5ct.runtimeClass, a6ct.runtimeClass, a7ct.runtimeClass), Array(a1, a2, a3, a4, a5, a6, a7))

  def invokeScalaObject[A1, A2, A3, A4, A5, A6, A7, A8, S](
    cls: Class[_], method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4], a5: Code[A5], a6: Code[A6], a7: Code[A7], a8: Code[A8])(
    implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], a5ct: ClassTag[A5], a6ct: ClassTag[A6], a7ct: ClassTag[A7], a8ct: ClassTag[A8], sct: ClassTag[S]
  ): Code[S] =
    invokeScalaObject[S](
      cls, method, Array[Class[_]](
        a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass, a5ct.runtimeClass, a6ct.runtimeClass, a7ct.runtimeClass, a8ct.runtimeClass), Array(a1, a2, a3, a4, a5, a6, a7, a8))

  def invokeScalaObject[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, S](
    cls: Class[_], method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4], a5: Code[A5], a6: Code[A6], a7: Code[A7], a8: Code[A8],
    a9: Code[A9], a10: Code[A10], a11: Code[A11], a12: Code[A12], a13: Code[A13])(
    implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], a5ct: ClassTag[A5], a6ct: ClassTag[A6], a7ct: ClassTag[A7],
    a8ct: ClassTag[A8], a9ct: ClassTag[A9], a10ct: ClassTag[A10], a11ct: ClassTag[A11], a12ct: ClassTag[A12], a13ct: ClassTag[A13], sct: ClassTag[S]): Code[S] =
    invokeScalaObject[S](
      cls, method,
      Array[Class[_]](
        a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass, a5ct.runtimeClass, a6ct.runtimeClass, a7ct.runtimeClass, a8ct.runtimeClass,
        a9ct.runtimeClass, a10ct.runtimeClass, a11ct.runtimeClass, a12ct.runtimeClass, a13ct.runtimeClass),
      Array(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13)
    )

  def invokeStatic[S](cls: Class[_], method: String, parameterTypes: Array[Class[_]], args: Array[Code[_]])(implicit sct: ClassTag[S]): Code[S] = {
    val m = Invokeable.lookupMethod(cls, method, parameterTypes)(sct)
    assert(m.isStatic)
    m.invoke(null, args)
  }

  def invokeStatic[T, S](method: String)(implicit tct: ClassTag[T], sct: ClassTag[S]): Code[S] =
    invokeStatic[S](tct.runtimeClass, method, Array[Class[_]](), Array[Code[_]]())

  def invokeStatic[T, A1, S](method: String, a1: Code[A1])(implicit tct: ClassTag[T], sct: ClassTag[S], a1ct: ClassTag[A1]): Code[S] =
    invokeStatic[S](tct.runtimeClass, method, Array[Class[_]](a1ct.runtimeClass), Array[Code[_]](a1))(sct)

  def invokeStatic[T, A1, A2, S](method: String, a1: Code[A1], a2: Code[A2])(implicit tct: ClassTag[T], sct: ClassTag[S], a1ct: ClassTag[A1], a2ct: ClassTag[A2]): Code[S] =
    invokeStatic[S](tct.runtimeClass, method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass), Array[Code[_]](a1, a2))(sct)

  def invokeStatic[T, A1, A2, A3, S](method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3])(implicit tct: ClassTag[T], sct: ClassTag[S], a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3]): Code[S] =
    invokeStatic[S](tct.runtimeClass, method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass), Array[Code[_]](a1, a2, a3))(sct)

  def invokeStatic[T, A1, A2, A3, A4, S](method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4])(implicit tct: ClassTag[T], sct: ClassTag[S], a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4]): Code[S] =
    invokeStatic[S](tct.runtimeClass, method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass), Array[Code[_]](a1, a2, a3, a4))(sct)

  def invokeStatic[T, A1, A2, A3, A4, A5, S](method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4], a5: Code[A5])(implicit tct: ClassTag[T], sct: ClassTag[S], a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], a5ct: ClassTag[A5]): Code[S] =
    invokeStatic[S](tct.runtimeClass, method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass, a5ct.runtimeClass), Array[Code[_]](a1, a2, a3, a4, a5))(sct)

  def _null[T >: Null]: Code[T] = Code(lir.insn(ACONST_NULL))

  def _empty: Code[Unit] = Code[Unit](null: lir.ValueX)

  def _throw[T <: java.lang.Throwable, U](cerr: Code[T]): Code[U] = Code(cerr, lir.insn1(ATHROW))

  def _fatal[U](msg: Code[String]): Code[U] =
    Code._throw[is.hail.utils.HailException, U](Code.newInstance[is.hail.utils.HailException, String, Option[String], Throwable](
      msg,
      Code.invokeStatic[scala.Option[String], scala.Option[String]]("empty"),
      Code._null[Throwable]))

  def _return[T](c: Code[T]): Code[Unit] = {
    c.end.append(if (c.v != null)
      lir.returnx(c.v)
    else
      lir.returnx())
    new Code(c.start, c.end, null)
  }

  def _println(c: Code[AnyRef]): Code[Unit] =
    Code.invokeScalaObject[AnyRef, Unit](scala.Console.getClass, "println", c)

  def checkcast[T](v: Code[AnyRef])(implicit tti: TypeInfo[T]): Code[T] =
    Code(v, lir.checkcast(tti.iname))

  def boxBoolean(cb: Code[Boolean]): Code[java.lang.Boolean] = Code.newInstance[java.lang.Boolean, Boolean](cb)

  def boxInt(ci: Code[Int]): Code[java.lang.Integer] = Code.newInstance[java.lang.Integer, Int](ci)

  def boxLong(cl: Code[Long]): Code[java.lang.Long] = Code.newInstance[java.lang.Long, Long](cl)

  def boxFloat(cf: Code[Float]): Code[java.lang.Float] = Code.newInstance[java.lang.Float, Float](cf)

  def boxDouble(cd: Code[Double]): Code[java.lang.Double] = Code.newInstance[java.lang.Double, Double](cd)

  def booleanValue(x: Code[java.lang.Boolean]): Code[Boolean] = toCodeObject(x).invoke[Boolean]("booleanValue")

  def intValue(x: Code[java.lang.Number]): Code[Int] = toCodeObject(x).invoke[Int]("intValue")

  def longValue(x: Code[java.lang.Number]): Code[Long] = toCodeObject(x).invoke[Long]("longValue")

  def floatValue(x: Code[java.lang.Number]): Code[Float] = toCodeObject(x).invoke[Float]("floatValue")

  def doubleValue(x: Code[java.lang.Number]): Code[Double] = toCodeObject(x).invoke[Double]("doubleValue")

  def getStatic[T: ClassTag, S: ClassTag : TypeInfo](field: String): Code[S] = {
    val f = FieldRef[T, S](field)
    assert(f.isStatic)
    f.getField(null)
  }

  def putStatic[T: ClassTag, S: ClassTag : TypeInfo](field: String, rhs: Code[S]): Code[Unit] = {
    val f = FieldRef[T, S](field)
    assert(f.isStatic)
    f.put(null, rhs)
  }

  def currentTimeMillis(): Code[Long] = Code.invokeStatic[java.lang.System, Long]("currentTimeMillis")

  def toUnit(c: Code[_]): Code[Unit] = new Code(c.start, c.end, null)
}

class Code[+T](
  val start: lir.Block,
  val end: lir.Block,
  val v: lir.ValueX) {
}

class Conditional(
  val start: lir.Block,
  val Ltrue: lir.Block,
  val Lfalse: lir.Block) {
  def unary_!(): Conditional = new Conditional(start, Lfalse, Ltrue)

  def &&(rhs: Conditional): Conditional = {
    Ltrue.append(lir.goto(rhs.start))
    rhs.Lfalse.append(lir.goto(Lfalse))
    new Conditional(start, rhs.Ltrue, Lfalse)
  }

  def ||(rhs: Conditional): Conditional = {
    Lfalse.append(lir.goto(rhs.start))
    rhs.Ltrue.append(lir.goto(Ltrue))
    new Conditional(start, Ltrue, rhs.Lfalse)
  }

  def toCode: Code[Boolean] = {
    val c = new lir.Local(null, "cond_to_bool", BooleanInfo)
    val L = new lir.Block()
    Ltrue.append(lir.store(c, lir.ldcInsn(1)))
    Ltrue.append(lir.goto(L))
    Lfalse.append(lir.store(c, lir.ldcInsn(0)))
    Lfalse.append(lir.goto(L))
    new Code(start, L, lir.load(c))
  }
}

class CodeBoolean(val lhs: Code[Boolean]) extends AnyVal {
  def toConditional: Conditional = {
    val Ltrue = new lir.Block()
    val Lfalse = new lir.Block()
    lhs.end.append(lir.ifx(IFNE, lhs.v, Ltrue, Lfalse))
    new Conditional(lhs.start, Ltrue, Lfalse)
  }

  def unary_!(): Code[Boolean] =
    (!lhs.toConditional).toCode

  def mux[T](cthen: Code[T], celse: Code[T]): Code[T] = {
    val cond = lhs.toConditional
    val L = new lir.Block()
    if (cthen.v == null) {
      cond.Ltrue.append(lir.goto(cthen.start))
      cthen.end.append(lir.goto(L))
      cond.Lfalse.append(lir.goto(celse.start))
      celse.end.append(lir.goto(L))
      new Code(cond.start, L, null)
    } else {
      assert(cthen.v.ti.desc == celse.v.ti.desc)
      val t = new lir.Local(null, "mux",
        cthen.v.ti)

      cond.Ltrue.append(lir.goto(cthen.start))
      cthen.end.append(lir.store(t, cthen.v))
      cthen.end.append(lir.goto(L))

      cond.Lfalse.append(lir.goto(celse.start))
      celse.end.append(lir.store(t, celse.v))
      celse.end.append(lir.goto(L))

      new Code(cond.start, L, lir.load(t))
    }
  }

  def orEmpty(cthen: Code[Unit]): Code[Unit] = {
    val cond = lhs.toConditional
    val L = new lir.Block()
    cond.Ltrue.append(lir.goto(cthen.start))
    cthen.end.append(lir.goto(L))
    cond.Lfalse.append(lir.goto(L))
    new Code(cond.start, L, null)
  }

  def &(rhs: Code[Boolean]): Code[Boolean] = Code(lhs, rhs, lir.insn2(IAND))

  def &&(rhs: Code[Boolean]): Code[Boolean] =
    (lhs.toConditional && rhs.toConditional).toCode

  def |(rhs: Code[Boolean]): Code[Boolean] = Code(lhs, rhs, lir.insn2(IOR))

  def ||(rhs: Code[Boolean]): Code[Boolean] =
    (lhs.toConditional || rhs.toConditional).toCode

  def ceq(rhs: Code[Boolean]): Code[Boolean] =
    lhs.toI.ceq(rhs.toI)

  def cne(rhs: Code[Boolean]): Code[Boolean] =
    lhs.toI.cne(rhs.toI)

  // on the JVM Booleans are represented as Ints
  def toI: Code[Int] = lhs.asInstanceOf[Code[Int]]

  def toS: Code[String] = lhs.mux(const("true"), const("false"))
}

class CodeInt(val lhs: Code[Int]) extends AnyVal {
  def unary_-(): Code[Int] = Code(lhs, lir.insn1(INEG))

  def +(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(IADD))

  def -(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(ISUB))

  def *(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(IMUL))

  def /(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(IDIV))

  def %(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(IREM))

  def compare(op: Int, rhs: Code[Int]): Code[Boolean] = Code(lhs, rhs, lir.boolean2(op))

  def >(rhs: Code[Int]): Code[Boolean] = lhs.compare(IF_ICMPGT, rhs)

  def >=(rhs: Code[Int]): Code[Boolean] = lhs.compare(IF_ICMPGE, rhs)

  def <(rhs: Code[Int]): Code[Boolean] = lhs.compare(IF_ICMPLT, rhs)

  def <=(rhs: Code[Int]): Code[Boolean] = lhs.compare(IF_ICMPLE, rhs)

  def >>(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(ISHR))

  def <<(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(ISHL))

  def >>>(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(IUSHR))

  def &(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(IAND))

  def |(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(IOR))

  def ^(rhs: Code[Int]): Code[Int] = Code(lhs, rhs, lir.insn2(IXOR))

  def unary_~(): Code[Int] = lhs ^ const(-1)

  def ceq(rhs: Code[Int]): Code[Boolean] = lhs.compare(IF_ICMPEQ, rhs)

  def cne(rhs: Code[Int]): Code[Boolean] = lhs.compare(IF_ICMPNE, rhs)

  def toI: Code[Int] = lhs

  def toL: Code[Long] = Code(lhs, lir.insn1(I2L))

  def toF: Code[Float] = Code(lhs, lir.insn1(I2F))

  def toD: Code[Double] = Code(lhs, lir.insn1(I2D))

  def toB: Code[Byte] = Code(lhs, lir.insn1(I2B))

  // on the JVM Booleans are represented as Ints
  def toZ: Code[Boolean] = lhs.asInstanceOf[Code[Boolean]]

  def toS: Code[String] = Code.invokeStatic[java.lang.Integer, Int, String]("toString", lhs)
}

class CodeLong(val lhs: Code[Long]) extends AnyVal {
  def unary_-(): Code[Long] = Code(lhs, lir.insn1(LNEG))

  def +(rhs: Code[Long]): Code[Long] = Code(lhs, rhs, lir.insn2(LADD))

  def -(rhs: Code[Long]): Code[Long] = Code(lhs, rhs, lir.insn2(LSUB))

  def *(rhs: Code[Long]): Code[Long] = Code(lhs, rhs, lir.insn2(LMUL))

  def /(rhs: Code[Long]): Code[Long] = Code(lhs, rhs, lir.insn2(LDIV))

  def %(rhs: Code[Long]): Code[Long] = Code(lhs, rhs, lir.insn2(LREM))

  def compare(rhs: Code[Long]): Code[Int] = Code(lhs, rhs, lir.insn2(LCMP))

  def <(rhs: Code[Long]): Code[Boolean] = compare(rhs) < 0

  def <=(rhs: Code[Long]): Code[Boolean] = compare(rhs) <= 0

  def >(rhs: Code[Long]): Code[Boolean] = compare(rhs) > 0

  def >=(rhs: Code[Long]): Code[Boolean] = compare(rhs) >= 0

  def ceq(rhs: Code[Long]): Code[Boolean] = compare(rhs) ceq 0

  def cne(rhs: Code[Long]): Code[Boolean] = compare(rhs) cne 0

  def >>(rhs: Code[Int]): Code[Long] = Code(lhs, rhs, lir.insn2(LSHR))

  def <<(rhs: Code[Int]): Code[Long] = Code(lhs, rhs, lir.insn2(LSHL))

  def >>>(rhs: Code[Int]): Code[Long] = Code(lhs, rhs, lir.insn2(LUSHR))

  def &(rhs: Code[Long]): Code[Long] = Code(lhs, rhs, lir.insn2(LAND))

  def |(rhs: Code[Long]): Code[Long] = Code(lhs, rhs, lir.insn2(LOR))

  def ^(rhs: Code[Long]): Code[Long] = Code(lhs, rhs, lir.insn2(LXOR))

  def unary_~(): Code[Long] = lhs ^ const(-1L)

  def toI: Code[Int] = Code(lhs, lir.insn1(L2I))

  def toL: Code[Long] = lhs

  def toF: Code[Float] = Code(lhs, lir.insn1(L2F))

  def toD: Code[Double] = Code(lhs, lir.insn1(L2D))

  def toS: Code[String] = Code.invokeStatic[java.lang.Long, Long, String]("toString", lhs)
}

class CodeFloat(val lhs: Code[Float]) extends AnyVal {
  def unary_-(): Code[Float] = Code(lhs, lir.insn1(FNEG))

  def +(rhs: Code[Float]): Code[Float] = Code(lhs, rhs, lir.insn2(FADD))

  def -(rhs: Code[Float]): Code[Float] = Code(lhs, rhs, lir.insn2(FSUB))

  def *(rhs: Code[Float]): Code[Float] = Code(lhs, rhs, lir.insn2(FMUL))

  def /(rhs: Code[Float]): Code[Float] = Code(lhs, rhs, lir.insn2(FDIV))

  def >(rhs: Code[Float]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(FCMPL)) > 0

  def >=(rhs: Code[Float]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(FCMPL)) >= 0

  def <(rhs: Code[Float]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(FCMPG)) < 0

  def <=(rhs: Code[Float]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(FCMPG)) <= 0

  def ceq(rhs: Code[Float]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(FCMPL)).ceq(0)

  def cne(rhs: Code[Float]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(FCMPL)).cne(0)

  def toI: Code[Int] = Code(lhs, lir.insn1(F2I))

  def toL: Code[Long] = Code(lhs, lir.insn1(F2L))

  def toF: Code[Float] = lhs

  def toD: Code[Double] = Code(lhs, lir.insn1(F2D))

  def toS: Code[String] = Code.invokeStatic[java.lang.Float, Float, String]("toString", lhs)
}

class CodeDouble(val lhs: Code[Double]) extends AnyVal {
  def unary_-(): Code[Double] = Code(lhs, lir.insn1(DNEG))

  def +(rhs: Code[Double]): Code[Double] = Code(lhs, rhs, lir.insn2(DADD))

  def -(rhs: Code[Double]): Code[Double] = Code(lhs, rhs, lir.insn2(DSUB))

  def *(rhs: Code[Double]): Code[Double] = Code(lhs, rhs, lir.insn2(DMUL))

  def /(rhs: Code[Double]): Code[Double] = Code(lhs, rhs, lir.insn2(DDIV))

  def >(rhs: Code[Double]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(DCMPL)) > 0

  def >=(rhs: Code[Double]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(DCMPL)) >= 0

  def <(rhs: Code[Double]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(DCMPG)) < 0

  def <=(rhs: Code[Double]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(DCMPG)) <= 0

  def ceq(rhs: Code[Double]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(DCMPL)).ceq(0)

  def cne(rhs: Code[Double]): Code[Boolean] = Code[Int](lhs, rhs, lir.insn2(DCMPL)).cne(0)

  def toI: Code[Int] = Code(lhs, lir.insn1(D2I))

  def toL: Code[Long] = Code(lhs, lir.insn1(D2L))

  def toF: Code[Float] = Code(lhs, lir.insn1(D2F))

  def toD: Code[Double] = lhs

  def toS: Code[String] = Code.invokeStatic[java.lang.Double, Double, String]("toString", lhs)
}

class CodeChar(val lhs: Code[Char]) extends AnyVal {
  def +(rhs: Code[Char]): Code[Char] = Code(lhs, rhs, lir.insn2(IADD))

  def -(rhs: Code[Char]): Code[Char] = Code(lhs, rhs, lir.insn2(ISUB))

  def >(rhs: Code[Int]): Code[Boolean] = lhs.toI > rhs.toI

  def >=(rhs: Code[Int]): Code[Boolean] = lhs.toI >= rhs.toI

  def <(rhs: Code[Int]): Code[Boolean] = lhs.toI < rhs.toI

  def <=(rhs: Code[Int]): Code[Boolean] = lhs.toI <= rhs

  def ceq(rhs: Code[Int]): Code[Boolean] = lhs.toI.ceq(rhs)

  def cne(rhs: Code[Int]): Code[Boolean] = lhs.toI.cne(rhs)

  def toI: Code[Int] = lhs.asInstanceOf[Code[Int]]

  def toS: Code[String] = Code.invokeStatic[java.lang.String, Char, String]("valueOf", lhs)
}

class CodeString(val lhs: Code[String]) extends AnyVal {
  def concat(other: Code[String]): Code[String] = lhs.invoke[String, String]("concat", other)

  def println(): Code[Unit] = Code.getStatic[System, PrintStream]("out").invoke[String, Unit]("println", lhs)

  def length(): Code[Int] = lhs.invoke[Int]("length")

  def apply(i: Code[Int]): Code[Char] = lhs.invoke[Int, Char]("charAt", i)
}

class CodeArray[T](val lhs: Code[Array[T]])(implicit tti: TypeInfo[T]) {
  def apply(i: Code[Int]): Code[T] =
    Code(lhs, i, lir.insn2(tti.aloadOp))

  def update(i: Code[Int], x: Code[T]): Code[Unit] = {
    lhs.start.append(lir.goto(i.end))
    i.start.append(lir.goto(x.start))
    x.end.append(lir.stmtOp(tti.astoreOp, lhs.v, i.v, x.v))
    new Code(lhs.start, x.end, null)
  }

  def length(): Code[Int] =
    Code(lhs, lir.insn1(ARRAYLENGTH))
}

object CodeLabel {
  def apply(): CodeLabel = {
    val L = new lir.Block()
    new CodeLabel(L)
  }
}

class CodeLabel(L: lir.Block) extends Code[Unit](L, L, null) {
  def goto: Code[Unit] = {
    val M = new lir.Block()
    M.append(lir.goto(L))
    new Code(M, M, null)
  }
}

object Invokeable {
  def apply[T](cls: Class[T], c: reflect.Constructor[_]): Invokeable[T, Unit] = new Invokeable[T, Unit](
    cls,
    "<init>",
    isStatic = false,
    isInterface = false,
    INVOKESPECIAL,
    Type.getConstructorDescriptor(c),
    implicitly[ClassTag[Unit]].runtimeClass)

  def apply[T, S](cls: Class[T], m: reflect.Method)(implicit sct: ClassTag[S]): Invokeable[T, S] = {
    val isInterface = m.getDeclaringClass.isInterface
    val isStatic = reflect.Modifier.isStatic(m.getModifiers)
    assert(!(isInterface && isStatic))
    new Invokeable[T, S](cls,
      m.getName,
      isStatic,
      isInterface,
      if (isInterface)
        INVOKEINTERFACE
      else if (isStatic)
        INVOKESTATIC
      else
        INVOKEVIRTUAL,
      Type.getMethodDescriptor(m),
      m.getReturnType)
  }

  def lookupMethod[T, S](cls: Class[T], method: String, parameterTypes: Array[Class[_]])(implicit sct: ClassTag[S]): Invokeable[T, S] = {
    val m = cls.getMethod(method, parameterTypes: _*)
    assert(m != null,
      s"no such method ${ cls.getName }.$method(${
        parameterTypes.map(_.getName).mkString(", ")
      })")

    // generic type parameters return java.lang.Object instead of the correct class
    assert(m.getReturnType.isAssignableFrom(sct.runtimeClass),
      s"when invoking ${ cls.getName }.$method(): ${ m.getReturnType.getName }: wrong return type ${ sct.runtimeClass.getName }")

    Invokeable(cls, m)
  }

  def lookupConstructor[T](cls: Class[T], parameterTypes: Array[Class[_]]): Invokeable[T, Unit] = {
    val c = cls.getDeclaredConstructor(parameterTypes: _*)
    assert(c != null,
      s"no such method ${ cls.getName }(${
        parameterTypes.map(_.getName).mkString(", ")
      })")

    Invokeable(cls, c)
  }
}

class Invokeable[T, S](tcls: Class[T],
  val name: String,
  val isStatic: Boolean,
  val isInterface: Boolean,
  val invokeOp: Int,
  val descriptor: String,
  val concreteReturnType: Class[_])(implicit sct: ClassTag[S]) {
  def invoke(lhs: Code[T], args: Array[Code[_]]): Code[S] = {
    val (start, end, argvs) = Code.sequenceValues(
      if (isStatic)
        args
      else
        lhs +: args)

    val sti = typeInfoFromClassTag(sct)

    // FIXME doesn't correctly handle units, generics
    if (sct.runtimeClass == java.lang.Void.TYPE) {
      end.append(
        lir.methodStmt(invokeOp, Type.getInternalName(tcls), name, descriptor, isInterface, sti, argvs))
      new Code(start, end, null)
    } else {
      var v = lir.methodInsn(invokeOp, Type.getInternalName(tcls), name, descriptor, isInterface, sti, argvs)
      if (concreteReturnType != sct.runtimeClass)
        v = lir.checkcast(Type.getInternalName(sct.runtimeClass), v)
      new Code(start, end, v)
    }
  }
}

object FieldRef {
  def apply[T, S](field: String)(implicit tct: ClassTag[T], sct: ClassTag[S], sti: TypeInfo[S]): FieldRef[T, S] = {
    val f = tct.runtimeClass.getDeclaredField(field)
    assert(f.getType == sct.runtimeClass,
      s"when getting field ${ tct.runtimeClass.getName }.$field: ${ f.getType.getName }: wrong type ${ sct.runtimeClass.getName } ")

    new FieldRef(f)
  }
}

trait Value[+T] { self =>
  def get: Code[T]
}

trait Settable[T] extends Value[T] {
  def store(rhs: Code[T]): Code[Unit]

  def :=(rhs: Code[T]): Code[Unit] = store(rhs)

  def storeAny(rhs: Code[_]): Code[Unit] = store(coerce[T](rhs))

  def load(): Code[T] = get
}

// FIXME lazy setup
class LazyFieldRef[T: TypeInfo](fb: FunctionBuilder[_], name: String, setup: => Code[T]) extends Settable[T] {
  private[this] val value: ClassFieldRef[T] = fb.newField[T](name)
  private[this] val present: ClassFieldRef[Boolean] = fb.newField[Boolean](s"${name}_present")

  def get: Code[T] =
    Code(present.mux(Code._empty, Code(value := setup, present := true)), value.load())

  def store(rhs: Code[T]): Code[Unit] =
    throw new UnsupportedOperationException("cannot store new value into LazyFieldRef!")
}

class ClassFieldRef[T: TypeInfo](fb: FunctionBuilder[_], f: Field[T]) extends Settable[T] {
  def name: String = f.name

  private def _loadClass: Value[java.lang.Object] = fb.getArg[java.lang.Object](0)

  def get: Code[T] = f.get(_loadClass)

  def store(rhs: Code[T]): Code[Unit] = f.put(_loadClass, rhs)
}

class LocalRef[T](val l: lir.Local)(implicit tti: TypeInfo[T]) extends Settable[T] {
  def get: Code[T] = Code(lir.load(l))

  def store(rhs: Code[T]): Code[Unit] = {
    rhs.end.append(lir.store(l, rhs.v))
    new Code(rhs.start, rhs.end, null)
  }
}

class LocalRefInt(val v: LocalRef[Int]) extends AnyRef {
  def +=(i: Int): Code[Unit] = {
    val L = new lir.Block()
    L.append(lir.iincInsn(v.l, i))
    new Code(L, L, null)
  }

  def ++(): Code[Unit] = +=(1)
}

class FieldRef[T, S](f: reflect.Field)(implicit tct: ClassTag[T], sti: TypeInfo[S]) {
  self =>

  val tiname = Type.getInternalName(tct.runtimeClass)

  def isStatic: Boolean = reflect.Modifier.isStatic(f.getModifiers)

  def getOp = if (isStatic) GETSTATIC else GETFIELD

  def putOp = if (isStatic) PUTSTATIC else PUTFIELD

  def getField(): Code[S] = getField(null: Value[T])

  def getField(lhs: Value[T]): Value[S] =
    new Value[S] {
      def get: Code[S] = self.getField(if (lhs != null) lhs.get else null)
    }

  def getField(lhs: Code[T]): Code[S] =
    if (isStatic)
      Code(lir.getStaticField(tiname, f.getName, sti))
    else
      Code(lhs, lir.getField(tiname, f.getName, sti))

  def put(lhs: Code[T], rhs: Code[S]): Code[Unit] =
    if (isStatic)
      Code.void(rhs, lir.putStaticField(tiname, f.getName, sti))
    else
      Code.void(lhs, rhs, lir.putField(tiname, f.getName, sti))
}

class CodeObject[T <: AnyRef : ClassTag](val lhs: Code[T]) {
  def getField[S](field: String)(implicit sct: ClassTag[S], sti: TypeInfo[S]): Code[S] =
    FieldRef[T, S](field).getField(lhs)

  def put[S](field: String, rhs: Code[S])(implicit sct: ClassTag[S], sti: TypeInfo[S]): Code[Unit] =
    FieldRef[T, S](field).put(lhs, rhs)

  def invokeConstructor(parameterTypes: Array[Class[_]], args: Array[Code[_]]): Code[Unit] =
    Invokeable.lookupConstructor[T](implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]], parameterTypes).invoke(lhs, args)

  def invoke[S](method: String, parameterTypes: Array[Class[_]], args: Array[Code[_]])
    (implicit sct: ClassTag[S]): Code[S] =
    Invokeable.lookupMethod[T, S](implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]], method, parameterTypes).invoke(lhs, args)

  def invoke[S](method: String)(implicit sct: ClassTag[S]): Code[S] =
    invoke[S](method, Array[Class[_]](), Array[Code[_]]())

  def invoke[A1, S](method: String, a1: Code[A1])(implicit a1ct: ClassTag[A1],
    sct: ClassTag[S]): Code[S] =
    invoke[S](method, Array[Class[_]](a1ct.runtimeClass), Array[Code[_]](a1))

  def invoke[A1, A2, S](method: String, a1: Code[A1], a2: Code[A2])(implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2],
    sct: ClassTag[S]): Code[S] =
    invoke[S](method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass), Array[Code[_]](a1, a2))

  def invoke[A1, A2, A3, S](method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3])
    (implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], sct: ClassTag[S]): Code[S] =
    invoke[S](method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass), Array[Code[_]](a1, a2, a3))

  def invoke[A1, A2, A3, A4, S](method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4])
    (implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], sct: ClassTag[S]): Code[S] =
    invoke[S](method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass), Array[Code[_]](a1, a2, a3, a4))

  def invoke[A1, A2, A3, A4, A5, S](method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4], a5: Code[A5])
    (implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], a5ct: ClassTag[A5], sct: ClassTag[S]): Code[S] =
    invoke[S](method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass, a5ct.runtimeClass), Array[Code[_]](a1, a2, a3, a4, a5))

  def invoke[A1, A2, A3, A4, A5, A6, A7, A8, S](method: String, a1: Code[A1], a2: Code[A2], a3: Code[A3], a4: Code[A4],
    a5: Code[A5], a6: Code[A6], a7: Code[A7], a8: Code[A8])
    (implicit a1ct: ClassTag[A1], a2ct: ClassTag[A2], a3ct: ClassTag[A3], a4ct: ClassTag[A4], a5ct: ClassTag[A5],
    a6ct: ClassTag[A6], a7ct: ClassTag[A7], a8ct: ClassTag[A8], sct: ClassTag[S]): Code[S] = {
    invoke[S](method, Array[Class[_]](a1ct.runtimeClass, a2ct.runtimeClass, a3ct.runtimeClass, a4ct.runtimeClass, a5ct.runtimeClass,
      a6ct.runtimeClass, a7ct.runtimeClass, a8ct.runtimeClass), Array[Code[_]](a1, a2, a3, a4, a5, a6, a7, a8))
  }
}

class CodeNullable[T >: Null : TypeInfo](val lhs: Code[T]) {
  def isNull: Code[Boolean] = Code(lhs, lir.boolean1(IFNULL))

  def ifNull[U](cnullcase: Code[U], cnonnullcase: Code[U]): Code[U] =
    isNull.mux(cnullcase, cnonnullcase)

  def mapNull[U >: Null](cnonnullcase: Code[U]): Code[U] =
    ifNull[U](Code._null[U], cnonnullcase)
}
