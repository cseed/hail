package is.hail.expr.ir

object Binds {
  def apply(x: IR, v: Sym, i: Int): Boolean = {
    x match {
      case Let(n, _, _) =>
        v == n && i == 1
      case ArrayMap(_, n, _) =>
        v == n && i == 1
      case ArrayFlatMap(_, n, _) =>
        v == n && i == 1
      case ArrayFilter(_, n, _) =>
        v == n && i == 1
      case ArrayFold(_, _, accumName, valueName, _) =>
        (v == accumName || v == valueName) && i == 2
      case ArrayScan(_, _, accumName, valueName, _) =>
        (v == accumName || v == valueName) && i == 2
      case AggExplode(_, n, _) =>
        v == n && i == 1
      case _ =>
        false
    }
  }
}
