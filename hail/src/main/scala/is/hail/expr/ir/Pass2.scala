package is.hail.expr.ir

import java.io.OutputStream

import is.hail.HailContext
import is.hail.annotations.Region
import is.hail.asm4s._
import is.hail.expr.ir.agg.Extract
import is.hail.expr.ir.lowering.{DArrayLowering, LowerToCDA, LowererUnsupportedOperation}
import is.hail.types.physical.{PTuple, PType}
import is.hail.types.virtual.TVoid
import is.hail.utils._

abstract class Pass2Type {
  type T

  def dump(t: T, os: OutputStream): Unit
}

object BaseIRPass2Type extends Pass2Type {
  type T = BaseIR

  def dump(ir: BaseIR, os: OutputStream): Unit = ???
}

abstract class BaseRawValue

case object RawVoidValue extends BaseRawValue
case class RawValue(pt: PTuple, a: Long)  extends BaseRawValue

object RawValuePass2Type extends Pass2Type {
  type T = BaseRawValue

  def dump(t: BaseRawValue, os: OutputStream): Unit = ???
}

object Pass2 {
  val optimize: Pass2 = new IteratedOptimizePass2(new SequencePass2(FastIndexedSeq(
    FoldConstantsPass2,
    ExtractIntervalFiltersPass2,
    SimplifyPass2,
    ForwardLetsPass2,
    ForwardRelationalLetsPass2,
    PruneDeadFieldsPass2)))

  val compileAndExecute: Pass2 = new SequencePass2(FastIndexedSeq(
    InlineApplyIRPass2,
    LowerArrayAggsToRunAggsPass2,
    optimize,
    CompileAndExecutePass2
  ))

  val default: Pass2 =
    new SequencePass2(FastIndexedSeq(
      optimize,
      LowerMatrixPass2,
      optimize,
      new TryLowering(
        new SequencePass2(FastIndexedSeq(
          LiftRelationalValuesToRelationalLetsPass2,
          LowerToCDAPass2,
          compileAndExecute)),
        new SequencePass2(FastIndexedSeq(
          InterpretNonCompilablePass2,
          compileAndExecute)))))
}

abstract class Pass2 {
  val inT: Pass2Type

  val outT: Pass2Type

  def runAny(ctx: ExecuteContext, in: Any): outT.T =
    run(ctx, in.asInstanceOf[inT.T])

  def run(ctx: ExecuteContext, in: inT.T): outT.T = {
    ctx.timer.time(stripTrailingDollar(getClass.getSimpleName)) {
      run1(ctx, in)
    }
  }

  def run1(ctx: ExecuteContext, in: inT.T): outT.T
}

abstract class BaseIRPass2 extends Pass2 {
  val inT: Pass2Type = BaseIRPass2Type

  val outT: Pass2Type = BaseIRPass2Type

  def run1(ctx: ExecuteContext, in: inT.T): outT.T =
    run2(ctx, in.asInstanceOf[BaseIR]).asInstanceOf[outT.T]

  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR
}

class SequencePass2(passes: IndexedSeq[Pass2]) extends Pass2 {
  assert(passes.nonEmpty)

  val inT: Pass2Type = passes(0).inT

  val outT: Pass2Type = passes(0).outT

  def run1(ctx: ExecuteContext, in: inT.T): outT.T = {
    var t: Any = in
    for (p <- passes)
      t = p.runAny(ctx, t)
    t.asInstanceOf[outT.T]
  }
}

class TryLowering(lowering: Pass2, fallback: Pass2) extends Pass2 {
  assert(lowering.inT eq fallback.inT)
  assert(lowering.outT eq fallback.outT)

  val inT: Pass2Type = lowering.inT

  val outT: Pass2Type = lowering.outT

  def run1(ctx: ExecuteContext, in: inT.T): outT.T = {
    try {
      lowering.runAny(ctx, in).asInstanceOf[outT.T]
    } catch {
      case _: LowererUnsupportedOperation =>
        fallback.runAny(ctx, in).asInstanceOf[outT.T]
    }
  }
}

object LowerMatrixPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    in match {
      case in: IR => LowerMatrixIR(in)
      case in: TableIR => LowerMatrixIR(in)
      case in: MatrixIR => LowerMatrixIR(in)
      case in: BlockMatrixIR => LowerMatrixIR(in)
    }
  }
}

object FoldConstantsPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    FoldConstants(ctx, in)
  }
}

object ExtractIntervalFiltersPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    ExtractIntervalFilters(in)
  }
}

object SimplifyPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    Simplify(in)
  }
}

object ForwardLetsPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    ForwardLets(in)
  }
}

object ForwardRelationalLetsPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    ForwardRelationalLets(in)
  }
}

object PruneDeadFieldsPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    PruneDeadFields(in)
  }
}

class IteratedOptimizePass2(pass: Pass2) extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    var prev: BaseIR = in
    var t: BaseIR = ctx.timer.time("0") {
      pass.runAny(ctx, in)
        .asInstanceOf[BaseIR]
    }
    val maxIterations = HailContext.get.optimizerIterations
    val i = 1
    while (i < maxIterations && prev != t) {
      prev = t
      t = pass.runAny(ctx, t)
        .asInstanceOf[BaseIR]
    }
    t
  }
}

object LiftRelationalValuesToRelationalLetsPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    LiftRelationalValues(in.asInstanceOf[IR])
  }
}

object LowerToCDAPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    val lowerTable = HailContext.getFlag("lower") != null
    val lowerBM = HailContext.getFlag("lower_bm") != null
    val typesToLower: DArrayLowering.Type = (lowerTable, lowerBM) match {
      case (true, true) => DArrayLowering.All
      case (true, false) => DArrayLowering.TableOnly
      case (false, true) => DArrayLowering.BMOnly
      case (false, false) => throw new LowererUnsupportedOperation("no lowering enabled")
    }
    LowerToCDA(in.asInstanceOf[IR], typesToLower, ctx)
  }
}

object InlineApplyIRPass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    RewriteBottomUp(in, {
      case x: ApplyIR => Some(x.explicitNode)
      case _ => None
    })
  }
}

object LowerArrayAggsToRunAggsPass2 extends BaseIRPass2 {
 def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    val r = Requiredness(in, ctx)
    RewriteBottomUp(in, {
      case x@StreamAgg(a, name, query) =>
        val res = genUID()
        val aggs = Extract(query, res, r)
        val newNode = Let(
          res,
          RunAgg(
            Begin(FastSeq(
              aggs.init,
              StreamFor(
                a,
                name,
                aggs.seqPerElt))),
            aggs.results,
            aggs.states),
          aggs.postAggIR)
        if (newNode.typ != x.typ)
          throw new RuntimeException(s"types differ:\n  new: ${ newNode.typ }\n  old: ${ x.typ }")
        Some(newNode)
      case x@StreamAggScan(a, name, query) =>
        val res = genUID()
        val aggs = Extract(query, res, r, isScan=true)
        val newNode = RunAggScan(
          a,
          name,
          aggs.init,
          aggs.seqPerElt,
          Let(res, aggs.results, aggs.postAggIR),
          aggs.states
        )
        if (newNode.typ != x.typ)
          throw new RuntimeException(s"types differ:\n  new: ${ newNode.typ }\n  old: ${ x.typ }")
        Some(newNode)
      case _ => None
    })
  }
}

object InterpretNonCompilablePass2 extends BaseIRPass2 {
  def run2(ctx: ExecuteContext, in: BaseIR): BaseIR = {
    InterpretNonCompilable(ctx, in)
  }
}

object CompileAndExecutePass2 extends Pass2 {
  val inT: Pass2Type = BaseIRPass2Type

  val outT: Pass2Type = RawValuePass2Type

  override def run1(ctx: ExecuteContext, in: inT.T): outT.T = {
    val ir = in.asInstanceOf[IR]

    if (!Compilable(ir))
      throw new LowererUnsupportedOperation(s"lowered to uncompilable IR: ${ Pretty(ir) }")

    val result: BaseRawValue =
      ir.typ match {
        case TVoid =>
          val (_, f) = ctx.timer.time("Compile") {
            Compile[AsmFunction1RegionUnit](ctx,
              FastIndexedSeq[(String, PType)](),
              FastIndexedSeq(classInfo[Region]), UnitInfo,
              ir)
          }
          ctx.timer.time("Run") {
            f(0, ctx.r)(ctx.r)
            RawVoidValue
          }
        case _ =>
          val (pt: PTuple, f) = ctx.timer.time("Compile") {
            Compile[AsmFunction1RegionLong](ctx,
              FastIndexedSeq[(String, PType)](),
              FastIndexedSeq(classInfo[Region]), LongInfo,
              MakeTuple.ordered(FastSeq(ir)))
          }
          ctx.timer.time("Run") {
            RawValue(pt, f(0, ctx.r).apply(ctx.r))
          }
      }
    result.asInstanceOf[outT.T]
  }
}
