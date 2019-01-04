package is.hail.methods

import is.hail.annotations._
import is.hail.expr.ir.{I, MatrixValue}
import is.hail.expr.ir.functions.MatrixToMatrixFunction
import is.hail.expr.types.MatrixType
import is.hail.expr.types.physical.{PLocus, PStruct}
import is.hail.expr.types.virtual.{TArray, TStruct}
import is.hail.rvd.{RVD, RVDType}
import is.hail.sparkextras.RepartitionedOrderedRDD2
import is.hail.variant.Locus
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._
import scala.collection.mutable

case class WindowByLocus(basePairs: Int) extends MatrixToMatrixFunction {
  def preservesPartitionCounts: Boolean = false

  def typeInfo(childType: MatrixType, childRVDType: RVDType): (MatrixType, RVDType) = {
    val newType = childType.copyParts(
      rowType = childType.rowType ++ TStruct(I("prev_rows") -> TArray(childType.rowType)),
      entryType = childType.entryType ++ TStruct(I("prev_entries") -> TArray(childType.entryType))
    )

    newType -> newType.canonicalRVDType
  }

  def execute(mv: MatrixValue): MatrixValue = {
    val (newType, rvdType) = typeInfo(mv.typ, mv.rvd.typ)

    val oldBounds = mv.rvd.partitioner.rangeBounds
    val adjBounds = oldBounds.map { interval =>
      val startLocus = interval.start.asInstanceOf[Row].getAs[Locus](0)
      val newStartLocus = startLocus.copy(position = startLocus.position - basePairs)
      interval.copy(start = Row(newStartLocus))
    }

    val localRVRowType = mv.rvd.rowPType
    val locusIndex = localRVRowType.fieldIdx(I("locus"))
    val keyOrdering = mv.typ.rowKeyStruct.ordering
    val localKeyFieldIdx = mv.rvd.typ.kFieldIdx
    val entriesIndex = MatrixType.getEntriesIndex(localRVRowType)
    val nonEntryIndices = (0 until localRVRowType.size).filter(_ != entriesIndex).toArray
    val entryArrayType = MatrixType.getEntryArrayType(localRVRowType)
    val entryType = entryArrayType.elementType.asInstanceOf[PStruct]
    val rg = localRVRowType.types(locusIndex).asInstanceOf[PLocus].rg

    val rangeBoundsBc = mv.sparkContext.broadcast(oldBounds)

    val nCols = mv.nCols

    val newRDD = RepartitionedOrderedRDD2(mv.rvd, adjBounds).cmapPartitionsWithIndex { (i, context, it) =>
      val bit = it.buffered

      val rb = new mutable.ArrayStack[Region]()

      def fetchRegion(): Region = {
        if (rb.isEmpty)
          context.freshRegion
        else
          rb.pop()
      }
      def recycleRegion(r: Region): Unit = {
        r.clear()
        rb.push(r)
      }

      val deque = new java.util.ArrayDeque[(Locus, RegionValue)]()

      val region = context.region
      val rv2 = RegionValue()
      val rvb = new RegionValueBuilder()

      def pushRow(locus: Locus, row: RegionValue) {
        val cpRegion = fetchRegion()
        rvb.set(cpRegion)
        rvb.clear()
        rvb.start(localRVRowType)
        rvb.startStruct()
        rvb.addAllFields(localRVRowType, row)
        rvb.endStruct()
        deque.push(locus -> RegionValue(cpRegion, rvb.end()))
      }

      def getLocus(row: RegionValue): Locus =
        UnsafeRow.readLocus(row.region, localRVRowType.loadField(row, locusIndex), rg)

      val unsafeRow = new UnsafeRow(localRVRowType)
      val keyView = new KeyedRow(unsafeRow, localKeyFieldIdx)
      while (bit.hasNext && { unsafeRow.set(bit.head); rangeBoundsBc.value(i).isAbovePosition(keyOrdering, keyView) }) {
        val rv = bit.next()
        pushRow(getLocus(rv), rv)
      }

      bit.map { rv =>
        val locus = UnsafeRow.readLocus(rv.region, localRVRowType.loadField(rv, locusIndex), rg)

        def discard(x: (Locus, RegionValue)): Boolean = x != null && (x._1.position < locus.position - basePairs
          || x._1.contig != locus.contig)

        while (discard(deque.peekLast()))
          recycleRegion(deque.removeLast()._2.region)

        val rvs = deque.iterator().asScala.map(_._2).toArray

        rvb.set(region)
        rvb.clear()
        rvb.start(rvdType.rowType)
        rvb.startStruct()
        rvb.addFields(localRVRowType, rv, nonEntryIndices)

        // prev_rows
        rvb.startArray(rvs.length)
        var j = 0
        while (j < rvs.length) {
          val rvj = rvs(j)
          rvb.startStruct()
          rvb.addFields(localRVRowType, rvj, nonEntryIndices)
          rvb.endStruct()
          j += 1
        }
        rvb.endArray()

        rvb.startArray(nCols)

        val entriesOffset = localRVRowType.loadField(rv, entriesIndex)
        val prevEntriesOffsets = rvs.map(localRVRowType.loadField(_, entriesIndex))

        j = 0
        while (j < nCols) {
          rvb.startStruct()
          if (entryArrayType.isElementDefined(rv.region, entriesOffset, j))
            rvb.addAllFields(entryType, rv.region, entryArrayType.loadElement(rv.region, entriesOffset, j))
          else
            rvb.skipFields(entryType.size)

          // prev_entries
          rvb.startArray(rvs.length)
          var k = 0
          while (k < rvs.length) {
            rvb.startStruct()
            if (entryArrayType.isElementDefined(rvs(k).region, prevEntriesOffsets(k), j))
              rvb.addAllFields(entryType, rvs(k).region, entryArrayType.loadElement(rvs(k).region, prevEntriesOffsets(k), j))
            else
              rvb.skipFields(entryType.size)
            rvb.endStruct()
            k += 1
          }
          rvb.endArray()
          rvb.endStruct()

          j += 1
        }
        rvb.endArray()
        rvb.endStruct()

        rv2.set(region, rvb.end())

        pushRow(locus, rv)
        rv2
      }
    }

    mv.copy(typ = newType,
      rvd = RVD(newType.canonicalRVDType, mv.rvd.partitioner, newRDD))
  }
}
