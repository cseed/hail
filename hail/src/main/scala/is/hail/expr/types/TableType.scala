package is.hail.expr.types

import is.hail.expr.ir._
import is.hail.expr.types.virtual.{TStruct, Type}
import is.hail.rvd.RVDType
import is.hail.utils._
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

class TableTypeSerializer extends CustomSerializer[TableType](format => (
  { case JString(s) => IRParser.parseTableType(s) },
  { case tt: TableType => JString(tt.toString) }))

case class TableType(rowType: TStruct, key: IndexedSeq[Sym], globalType: TStruct) extends BaseType {
  val canonicalRVDType = RVDType(rowType.physicalType, key)

  def env: Env[Type] = {
    Env.empty[Type]
      .bind((GlobalSym, globalType))
      .bind((RowSym, rowType))
  }

  def globalEnv: Env[Type] = Env.empty[Type]
    .bind(GlobalSym -> globalType)

  def rowEnv: Env[Type] = Env.empty[Type]
    .bind(GlobalSym -> globalType)
    .bind(RowSym -> rowType)

  def refMap: Map[Sym, Type] = Map(
    GlobalSym -> globalType,
    RowSym -> rowType)

  def keyType: TStruct = canonicalRVDType.kType.virtualType
  val keyFieldIdx: Array[Int] = canonicalRVDType.kFieldIdx
  def valueType: TStruct = canonicalRVDType.valueType.virtualType
  val valueFieldIdx: Array[Int] = canonicalRVDType.valueFieldIdx

  def pretty(sb: StringBuilder, indent0: Int = 0, compact: Boolean = false) {
    var indent = indent0

    val space: String = if (compact) "" else " "

    def newline() {
      if (!compact) {
        sb += '\n'
        sb.append(" " * indent)
      }
    }

    sb.append(s"Table$space{")
    indent += 4
    newline()

    sb.append(s"global:$space")
    globalType.pretty(sb, indent, compact)
    sb += ','
    newline()

    sb.append(s"key:$space[")
    key.foreachBetween(k => sb.append(k))(sb.append(s",$space"))
    sb += ']'
    sb += ','
    newline()

    sb.append(s"row:$space")
    rowType.pretty(sb, indent, compact)

    indent -= 4
    newline()
    sb += '}'
  }
}
