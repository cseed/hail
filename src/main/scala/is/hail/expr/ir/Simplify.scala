package is.hail.expr.ir

import is.hail.utils._
import is.hail.expr.{BaseIR, FilterColsIR, FilterRowsIR, MatrixRead, TableFilter, TableRead, TableUnion}

object Simplify {
  def apply(ir: BaseIR): BaseIR = {
    RewriteBottomUp(ir, matchErrorToNone {
      // optimze IR

      case Let(n1, v, Ref(n2, _)) if n1 == n2 => v

      // If(NA, ...) handled by FoldConstants
      case If(True(), x, _) => x
      case If(False(), _, x) => x

      case If(c, cnsq, altr) if cnsq == altr =>
        If(IsNA(c), NA(cnsq.typ), cnsq)

      case Cast(x, t) if x.typ == t => x

      case ArrayLen(MakeArray(args, _)) => I32(args.length)

      case ArrayRef(MakeArray(args, _), I32(i)) => args(i)

      case ArrayFilter(a, _, True()) => a

      case AggFilter(a, _, True()) => a

      case GetField(MakeStruct(fields), name) =>
        val (_, x) = fields.find { case (n, _) => n == name }.get
        x

      case GetField(InsertFields(old, fields), name) =>
        fields.find { case (n, _) => n == name } match {
          case Some((_, x)) => x
          case None => GetField(old, name)
        }

      case GetTupleElement(MakeTuple(xs), idx) => xs(idx)

      // optimize TableIR
      case TableFilter(t, True()) => t

      case TableFilter(TableRead(path, spec, _), False() | NA(_)) =>
        TableRead(path, spec, dropRows = true)

      case TableFilter(TableFilter(t, p1), p2) =>
        TableFilter(t,
          ApplySpecial("&&", Array(p1, p2)))

        // flatten unions
      case TableUnion(children) if children.exists(_.isInstanceOf[TableUnion]) =>
        TableUnion(children.flatMap {
          case u: TableUnion => u.children
          case c => Some(c)
        })

      // optimize MatrixIR

      // Equivalent rewrites for the new Filter{Cols,Rows}IR
      case FilterRowsIR(MatrixRead(path, spec, dropCols,  _), False() | NA(_)) =>
        MatrixRead(path, spec, dropCols, dropRows = true)

      case FilterColsIR(MatrixRead(path, spec, _, dropRows), False() | NA(_)) =>
        MatrixRead(path, spec, dropCols = true, dropRows)

      // Keep all rows/cols = do nothing
      case FilterRowsIR(m, True()) => m

      case FilterColsIR(m, True()) => m
    })
  }
}
