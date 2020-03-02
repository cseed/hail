package is.hail.lir

import java.io.{PrintWriter, StringWriter}

import is.hail.utils._
import org.objectweb.asm.{ClassReader, ClassWriter}
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.tree._
import org.objectweb.asm.util.{CheckClassAdapter, Textifier, TraceClassVisitor}

import scala.collection.JavaConverters._
import scala.collection.mutable

object Emit {
  def asBytes(cn: ClassNode, print: Option[PrintWriter]): Array[Byte] = {
    val cw = new ClassWriter(ClassWriter.COMPUTE_MAXS + ClassWriter.COMPUTE_FRAMES)
    val sw1 = new StringWriter()
    var bytes: Array[Byte] = new Array[Byte](0)
    try {
      for (method <- cn.methods.asInstanceOf[java.util.List[MethodNode]].asScala) {
        val count = method.instructions.size
        log.info(s"instruction count: $count: ${ cn.name }.${ method.name }")
        if (count > 8000)
          log.warn(s"big method: $count: ${ cn.name }.${ method.name }")
      }

      cn.accept(cw)
      bytes = cw.toByteArray
      //       This next line should always be commented out!
      //      CheckClassAdapter.verify(new ClassReader(bytes), false, new PrintWriter(sw1))
    } catch {
      case e: Exception =>
        // if we fail with frames, try without frames for better error message
        val cwNoFrames = new ClassWriter(ClassWriter.COMPUTE_MAXS)
        val sw2 = new StringWriter()
        cn.accept(cwNoFrames)
        try {
          CheckClassAdapter.verify(new ClassReader(cwNoFrames.toByteArray), false, new PrintWriter(sw2))
        } catch {
          case e: Exception =>
            log.error("Verify Output 1 for " + cn.name + ":")
            throw e
        }

        if (sw2.toString.length() != 0) {
          System.err.println("Verify Output 2 for " + cn.name + ":")
          System.err.println(sw2)
          throw new IllegalStateException("Bytecode failed verification 1", e)
        } else {
          if (sw1.toString.length() != 0) {
            System.err.println("Verify Output 1 for " + cn.name + ":")
            System.err.println(sw1)
          }
          throw e
        }
    }

    if (sw1.toString.length != 0) {
      System.err.println("Verify Output 1 for " + cn.name + ":")
      System.err.println(sw1)
      throw new IllegalStateException("Bytecode failed verification 2")
    }

    print.foreach { pw =>
      val cr = new ClassReader(bytes)
      val tcv = new TraceClassVisitor(null, new Textifier, pw)
      cr.accept(tcv, 0)
    }
    bytes
  }

  def emit(cn: ClassNode, m: Method): Unit = {
    val mn = new MethodNode(ACC_PUBLIC, m.name, m.desc, null, null)
    cn.methods.asInstanceOf[java.util.List[MethodNode]].add(mn)

    val labelNodes = m.blocks.map(L => L -> new LabelNode).toMap

    val localIndex: mutable.Map[Local, Int] = mutable.Map[Local, Int]()

    val start = new LabelNode
    val end = new LabelNode

    var n = 0
    val parameterIndex = new Array[Int](m.parameterTypeInfo.length + 1)
    var i = 0
    while (i < parameterIndex.length) {
      parameterIndex(i) = n
      if (i == 0)
        n += 1 // this
      else
        n += m.parameterTypeInfo(i - 1).slots
      i += 1
    }

    val locals: mutable.Set[Local] = mutable.Set()
    for (ell <- m.blocks) {
      def findLocals(x: X): Unit = {
        x match {
          case x: StoreX =>
            locals += x.l
          case x: LoadX =>
            locals += x.l
          case x: IincX =>
            locals += x.l
          case _ =>
        }

        x.children.map(findLocals)
      }

      var x = ell.first
      while (x != null) {
        findLocals(x)
        x = x.next
      }
    }

    for (l <- locals) {
      if (!l.isInstanceOf[Parameter]) {
        val i = n
        localIndex += (l -> i)
        mn.localVariables.asInstanceOf[java.util.List[LocalVariableNode]]
          .add(new LocalVariableNode(l.name, l.ti.desc, null, start, end, i))
        n += l.ti.slots
      }
    }

    def getLocalIndex(l: Local): Int = {
      l match {
        case p: Parameter => parameterIndex(p.i)
        case _ =>localIndex(l)
      }
    }

    def emitX(x: X): Unit = {
      x.children.foreach(emitX(_))

      x match {
        case x: GotoX =>
          mn.instructions.add(new JumpInsnNode(GOTO, labelNodes(x.L)))
        case x: ReturnX =>
          mn.instructions.add(new InsnNode(
            if (x.children.length == 0)
              RETURN
            else
              m.returnTypeInfo.returnOp))
        case x: LoadX =>
          mn.instructions.add(new VarInsnNode(x.l.ti.loadOp, getLocalIndex(x.l)))
        case x: StoreX =>
          mn.instructions.add(new VarInsnNode(x.l.ti.storeOp, getLocalIndex(x.l)))
        case x: InsnX =>
          mn.instructions.add(new InsnNode(x.op))
        case x: TypeInsnX =>
          mn.instructions.add(new TypeInsnNode(x.op, x.t))
        case x: MethodX =>
          mn.instructions.add(new MethodInsnNode(x.op,
            x.method.owner, x.method.name, x.method.desc, x.method.isInterface))
        case x: MethodStmtX =>
          mn.instructions.add(new MethodInsnNode(x.op,
            x.method.owner, x.method.name, x.method.desc, x.method.isInterface))
        case x: NewInstanceX =>
          mn.instructions.add(new TypeInsnNode(NEW, x.ti.iname))
        case x: LdcX =>
          mn.instructions.add(new LdcInsnNode(x.a))
      }
    }

    def emitBlock(L: Block): Unit = {
      mn.instructions.add(labelNodes(L))
      var x = L.first
      while (x != null) {
        emitX(x)
        x = x.next
      }
    }

    mn.instructions.add(start)
    emitBlock(m.entry)
    for (b <- m.blocks) {
      if (b ne m.entry)
        emitBlock(b)
    }
    mn.instructions.add(end)
  }

  def apply(c: Classx[_], print: Option[PrintWriter]): Array[Byte] = {
    val cn = new ClassNode()

    cn.version = V1_8
    cn.access = ACC_PUBLIC

    cn.name = c.name
    cn.superName = c.superName
    for (intf <- c.interfaces)
      cn.interfaces.asInstanceOf[java.util.List[String]].add(intf)

    for (m <- c.methods) {
      emit(cn, m)
    }

    asBytes(cn, print)
  }
}
