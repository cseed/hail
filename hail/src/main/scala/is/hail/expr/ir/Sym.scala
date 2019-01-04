package is.hail.expr.ir

import is.hail.utils.StringEscapeUtils

// FIXME Sym instead of Symbol so we don't conflict with Scala's Symbol which is currently used by by the IR DSL
object Sym {
  var counter: Int = 0

  def gen(base: String): Sym = {
    counter += 1
    Generated("j", base, counter)
  }
}

abstract class Sym

object I {
  def apply(name: String): Sym = Identifier(name)
}

case class Identifier(name: String) extends Sym {
  override def toString: String = {
    if (name.matches("""\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*"""))
      name
    else
      s"`${ StringEscapeUtils.escapeString(name, backticked = true) }`"
  }
}

// lang is one of "py" or "j"
case class Generated(lang: String, base: String, i: Int) extends Sym {
  override def toString: String = s":$lang$base-$i"
}

case object GlobalSym extends Sym {
  override def toString: String = ":global"
}

case object ColSym extends Sym {
  override def toString: String = ":col"
}

case object RowSym extends Sym {
  override def toString: String = ":row"
}

// used by TableParallelize
case object RowsSym extends Sym {
  override def toString: String = ":rows"
}

case object EntrySym extends Sym {
  override def toString: String = ":entry"
}

case object ColsSym extends Sym {
  override def toString: String = ":cols"
}

case object GlobalAndColsSym extends Sym {
  override def toString: String = ":global-and-cols"
}

case object EntriesSym extends Sym {
  override def toString: String = ":entries"
}

case object AGGRSym extends Sym {
  override def toString: String = ":AGGR"
}

case object SCANRSym extends Sym {
  override def toString: String = ":SCANR"
}