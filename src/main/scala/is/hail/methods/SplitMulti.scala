package is.hail.methods

import is.hail.annotations._
import is.hail.expr._
import is.hail.sparkextras.{OrderedRDD, OrderedRDD2}
import is.hail.utils._
import is.hail.variant.{Genotype, GenotypeBuilder, HTSGenotypeView, Locus, TGenotypeView, Variant, VariantDataset}
import org.apache.spark.rdd.RDD

object SplitMulti {

  def splitGT(gt: Int, i: Int): Int = {
    val p = Genotype.gtPair(gt)
    (if (p.j == i) 1 else 0) +
      (if (p.k == i) 1 else 0)
  }

  def split(prevLocus: Locus, nSamples: Int, rowType: TStruct, rv: RegionValue,
    newRowType: TStruct, rvb: RegionValueBuilder, splitRegion: MemoryBuffer, splitrv: RegionValue,
    propagateGQ: Boolean,
    keepStar: Boolean,
    insertSplitAnnots: (Annotation, Int, Boolean) => Annotation,
    sortAlleles: Boolean,
    filterMinrepped: Boolean, filterMoving: Boolean, verifyMinrepped: Boolean): Iterator[RegionValue] = {
    require(!(filterMoving && verifyMinrepped))

    val ur = new UnsafeRow(rowType, rv.region, rv.offset)

    val v = ur.getAs[Variant](1)

    var minrepped = true
    if (prevLocus != null && prevLocus == v.locus)
      minrepped = false
    var splitVariants = v.altAlleles.iterator.zipWithIndex
      .filter(keepStar || !_._1.isStar)
      .map { case (aa, aai) =>
        val splitv = Variant(v.contig, v.start, v.ref, Array(aa))
        val minsplitv = splitv.minRep

        if (splitv != minsplitv)
          minrepped = false

        (minsplitv, aai + 1)
      }
      .toArray

    if (splitVariants.isEmpty)
      return Iterator()

    if (minrepped) {
      if (filterMinrepped)
        return Iterator()
    } else {
      if (filterMoving)
        return Iterator()
      else if (verifyMinrepped)
        fatal("found non-minrepped variant: $v")
    }

    val va = ur.get(2)

    if (v.isBiallelic) {
      val (minrep, _) = splitVariants(0)
      assert(v.minRep == minrep)

      rvb.set(rv.region)
      rvb.start(newRowType)
      rvb.startStruct()
      rvb.addAnnotation(newRowType.fieldType(0), minrep.locus) // pk
      rvb.addAnnotation(newRowType.fieldType(1), minrep) // pk

      val newVA = insertSplitAnnots(va, 1, false)
      rvb.addAnnotation(newRowType.fieldType(2), newVA)

      rvb.addField(rowType, rv, 3) // gs
      rvb.endStruct()
      splitrv.set(rv.region, rvb.end())

      return Iterator(splitrv)
    }

    if (sortAlleles)
      splitVariants = splitVariants.sortBy { case (svj, i) => svj }

    val nAlleles = v.nAlleles
    val nGenotypes = v.nGenotypes

    splitVariants.iterator.zipWithIndex
      .map { case ((svj, i), j) =>
        splitRegion.clear()
        rvb.set(splitRegion)
        rvb.start(newRowType)
        rvb.startStruct()
        rvb.addAnnotation(newRowType.fieldType(0), svj.locus)
        rvb.addAnnotation(newRowType.fieldType(1), svj)

        val newVA = insertSplitAnnots(va, i, true)
        rvb.addAnnotation(newRowType.fieldType(2), newVA)

        val g = HTSGenotypeView(rowType).asInstanceOf[TGenotypeView]
        g.setRegion(rv.region, rv.offset)

        val px = new Array[Int](3)
        rvb.startArray(nSamples)

        var k = 0
        while (k < nSamples) {
          g.setGenotype(k)
          if (g.gIsDefined) {
            rvb.startStruct() // g
            if (!g.isLinearScale) {
              var fakeRef: Boolean = false

              if (g.hasGT) {
                val gt = g.getGT
                val sgt = splitGT(gt, i)
                rvb.addInt(sgt)

                val p = Genotype.gtPair(gt)
                if (sgt != p.nNonRefAlleles)
                  fakeRef = true
              } else
                rvb.setMissing()

              if (g.hasAD) {
                var sum = 0
                var l = 0
                while (l < nAlleles) {
                  sum += g.getAD(l)
                  l += 1
                }

                rvb.startArray(2)
                rvb.addInt(sum - g.getAD(i))
                rvb.addInt(g.getAD(i))
                rvb.endArray()
              } else
                rvb.setMissing()

              if (g.hasDP)
                rvb.addInt(g.getDP)
              else
                rvb.setMissing()

              if (g.hasPL) {
                px(0) = Int.MaxValue
                px(1) = Int.MaxValue
                px(2) = Int.MaxValue

                var l = 0
                while (l < nGenotypes) {
                  val sgt = splitGT(l, i)
                  val pll = g.getPL(l)
                  if (pll < px(sgt))
                    px(sgt) = pll
                  l += 1
                }
              }

              if (propagateGQ) {
                if (g.hasGQ)
                  rvb.addInt(g.getGQ)
                else
                  rvb.setMissing()
              } else {
                if (g.hasPL) {
                  val gq = Genotype.gqFromPL(px)
                  rvb.addInt(gq)
                } else
                  rvb.setMissing()
              }

              if (g.hasPL) {
                rvb.startArray(3)
                rvb.addInt(px(0))
                rvb.addInt(px(1))
                rvb.addInt(px(2))
                rvb.endArray()
              } else
                rvb.setMissing()

              rvb.addBoolean(fakeRef)
              rvb.addBoolean(false) // !isLinearScale
            } else {
              if (g.hasPX) {
                px(0) = 0
                px(1) = 0
                px(2) = 0

                var l = 0
                while (l < nGenotypes) {
                  val sgt = splitGT(l, i)
                  val pxl = g.getPX(l)
                  px(sgt) += pxl
                  l += 1
                }

                // FIXME allocates
                val newpx = Genotype.weightsToLinear(px)
                val newgt = Genotype.unboxedGTFromLinear(newpx)

                if (newgt != -1)
                  rvb.addInt(newgt)
                else
                  rvb.setMissing()

                rvb.setMissing() // ad
                rvb.setMissing() // dp
                rvb.setMissing() // gq

                rvb.startArray(3)
                rvb.addInt(newpx(0))
                rvb.addInt(newpx(1))
                rvb.addInt(newpx(2))
                rvb.endArray()

                if (g.hasGT) {
                  val gt = g.getGT
                  val p = Genotype.gtPair(gt)
                  if (newgt != p.nNonRefAlleles && newgt != -1)
                    rvb.addBoolean(true)
                  else
                    rvb.addBoolean(false)
                } else
                  rvb.addBoolean(false)

                rvb.addBoolean(true)
              } else {
                // all fields missing
                rvb.setMissing() // gt
                rvb.setMissing() // ad
                rvb.setMissing() // dp
                rvb.setMissing() // gq
                rvb.setMissing() // pl
                rvb.setMissing() // fakeRef
                rvb.addBoolean(true) // isLinearScale
              }
            }

            rvb.endStruct()
          } else
            rvb.setMissing()

          k += 1
        }
        rvb.endArray() // gs

        rvb.endStruct()
        splitrv.set(splitRegion, rvb.end())
        splitrv
      }
  }

  def split(vds: VariantDataset,
    newMatrixType: MatrixType, insertSplitAnnots: (Annotation, Int, Boolean) => Annotation,
    propagateGQ: Boolean, keepStar: Boolean, sortAlleles: Boolean,
    filterMinrepped: Boolean, filterMoving: Boolean, verifyMinrepped: Boolean): RDD[RegionValue] = {

    val localNSamples = vds.nSamples
    val localRowType = vds.rowType

    val newRowType = newMatrixType.rowType

    vds.rdd2.mapPartitions { it =>
      var prevLocus: Locus = null
      val rvb = new RegionValueBuilder()
      val splitRegion = MemoryBuffer()
      val splitrv = RegionValue()

      it.flatMap { rv =>
        val splitit = split(prevLocus, localNSamples, localRowType, rv,
          newRowType, rvb, splitRegion, splitrv,
          propagateGQ = propagateGQ,
          keepStar = keepStar,
          insertSplitAnnots = insertSplitAnnots,
          sortAlleles = sortAlleles,
          filterMinrepped = filterMinrepped, filterMoving = filterMoving, verifyMinrepped = verifyMinrepped)

        val ur = new UnsafeRow(localRowType, rv.region, rv.offset)
        prevLocus = ur.getAs[Locus](0)

        splitit
      }
    }
  }

  def splitNumber(str: String): String =
    if (str == "A" || str == "R" || str == "G")
      "."
    else
      str

  def apply(vds: VariantDataset, propagateGQ: Boolean = false, keepStar: Boolean = false,
    minrepped: Boolean = false): VariantDataset = {

    if (vds.wasSplit) {
      warn("called redundant split on an already split VDS")
      return vds
    }

    val (vas2, insertIndex) = vds.vaSignature.insert(TInt32, "aIndex")
    val (vas3, insertSplit) = vas2.insert(TBoolean, "wasSplit")

    val vas4 = vas3.getAsOption[TStruct]("info").map { s =>
      val updatedInfoSignature = TStruct(s.fields.map { f =>
        f.attrs.get("Number").map(splitNumber) match {
          case Some(n) => f.copy(attrs = f.attrs + ("Number" -> n))
          case None => f
        }
      })
      val (newSignature, _) = vas3.insert(updatedInfoSignature, "info")
      newSignature
    }.getOrElse(vas3)

    val newMatrixType = vds.matrixType.copy(vaType = vas4)

    val insertSplitAnnots: (Annotation, Int, Boolean) => Annotation =
      (va, index, wasSplit) => insertSplit(insertIndex(va, index), wasSplit)

    val newRDD2: OrderedRDD2 =
      if (minrepped) {
        OrderedRDD2(
          newMatrixType.orderedRDD2Type,
          vds.rdd2.orderedPartitioner,
          split(vds, newMatrixType, insertSplitAnnots,
            propagateGQ = propagateGQ, keepStar = keepStar,
            sortAlleles = true, filterMinrepped = false, filterMoving = false, verifyMinrepped = true))
      } else {
        val minrepped = OrderedRDD2(
          newMatrixType.orderedRDD2Type,
          vds.rdd2.orderedPartitioner,
          split(vds, newMatrixType, insertSplitAnnots,
            propagateGQ = propagateGQ, keepStar = keepStar,
            sortAlleles = true, filterMinrepped = false, filterMoving = true, verifyMinrepped = false))

        val moved = OrderedRDD2.shuffle(
          newMatrixType.orderedRDD2Type,
          vds.rdd2.orderedPartitioner,
          split(vds, newMatrixType, insertSplitAnnots,
            propagateGQ = propagateGQ, keepStar = keepStar,
            sortAlleles = false, filterMinrepped = true, filterMoving = false, verifyMinrepped = false))

        minrepped.partitionSortedUnion(moved)
      }

    vds.copy2(rdd2 = newRDD2, vaSignature = vas4, wasSplit = true)
  }
}
