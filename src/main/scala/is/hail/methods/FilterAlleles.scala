package is.hail.methods

import is.hail.annotations._
import is.hail.expr.{EvalContext, Parser, TArray, TInt32, TVariant}
import is.hail.sparkextras.{OrderedRDD, OrderedRDD2}
import is.hail.utils._
import is.hail.variant.{GTPair, GenomeReference, Genotype, Locus, Variant, VariantDataset}
import org.apache.spark.rdd.RDD

import scala.math.min

object FilterAlleles {

  def apply(vds: VariantDataset, filterExpr: String, annotationExpr: String = "va = va",
    filterAlteredGenotypes: Boolean = false, keep: Boolean = true,
    subset: Boolean = true, minrepped: Boolean = false, keepStar: Boolean = false): VariantDataset = {

    if (vds.wasSplit)
      warn("this VDS was already split; this module was designed to handle multi-allelics, perhaps you should use filtervariants instead.")

    val conditionEC = EvalContext(Map(
      "v" -> (0, TVariant(GenomeReference.GRCh37)),
      "va" -> (1, vds.vaSignature),
      "aIndex" -> (2, TInt32)))
    val conditionE = Parser.parseTypedExpr[java.lang.Boolean](filterExpr, conditionEC)

    val annotationEC = EvalContext(Map(
      "v" -> (0, TVariant(GenomeReference.GRCh37)),
      "va" -> (1, vds.vaSignature),
      "aIndices" -> (2, TArray(TInt32))))

    val (paths, types, f) = Parser.parseAnnotationExprs(annotationExpr, annotationEC, Some(Annotation.VARIANT_HEAD))
    val inserterBuilder = new ArrayBuilder[Inserter]()
    val newVAType = (paths, types).zipped.foldLeft(vds.vaSignature) { case (vas, (path, signature)) =>
      val (newVas, i) = vas.insert(signature, path)
      inserterBuilder += i
      newVas
    }
    val inserters = inserterBuilder.result()

    val localNSamples = vds.nSamples

    val newMatrixType = vds.matrixType.copy(vaType = newVAType)

    def filterAllelesInVariant(v: Variant, va: Annotation): Option[(Variant, IndexedSeq[Int], Array[Int])] = {
      var alive = 0
      val oldToNew = new Array[Int](v.nAlleles)
      for (aai <- v.altAlleles.indices) {
        val index = aai + 1
        conditionEC.setAll(v, va, index)
        oldToNew(index) =
          if (Filter.boxedKeepThis(conditionE(), keep)) {
            alive += 1
            alive
          } else
            0
      }

      if (alive == 0)
        None
      else {
        val newToOld = oldToNew.iterator
          .zipWithIndex
          .filter { case (newIdx, oldIdx) => oldIdx == 0 || newIdx != 0 }
          .map(_._2)
          .toArray

        val altAlleles = oldToNew.iterator
          .zipWithIndex
          .filter { case (newIdx, _) => newIdx != 0 }
          .map { case (_, idx) => v.altAlleles(idx - 1) }
          .toArray

        Some((v.copy(altAlleles = altAlleles).minRep, newToOld: IndexedSeq[Int], oldToNew))
      }
    }

    def updateAnnotation(v: Variant, va: Annotation, newToOld: IndexedSeq[Int]): Annotation = {
      annotationEC.setAll(v, va, newToOld)
      f().zip(inserters).foldLeft(va) { case (va, (v, inserter)) => inserter(va, v) }
    }

    def updateGenotypes(rvb: RegionValueBuilder, gs: IndexedSeq[Genotype], oldToNew: Array[Int], newCount: Int) {
      def downcodeGtPair(gt: GTPair): GTPair =
        GTPair.fromNonNormalized(oldToNew(gt.j), oldToNew(gt.k))

      def downcodeGT(gt: Int): Int =
        Genotype.gtIndex(downcodeGtPair(Genotype.gtPair(gt)))

      def downcodeAD(ad: Array[Int]): Array[Int] = {
        coalesce(ad)(newCount, (_, alleleIndex) => oldToNew(alleleIndex), 0) { (oldDepth, depth) =>
          oldDepth + depth
        }
      }

      def downcodePX(px: Array[Int]): Array[Int] = {
        coalesce(px)(triangle(newCount), (_, gt) => downcodeGT(gt), Int.MaxValue) { (oldNll, nll) =>
          min(oldNll, nll)
        }
      }

      def downcodeGenotype(g: Genotype) {
        val newPX = Genotype.px(g).map(downcodePX)

        var newGT = Genotype.gt(g).map(downcodeGT)
        if (filterAlteredGenotypes && newGT != Genotype.gt(g))
          newGT = None
        rvb.startStruct()
        newGT match {
          case Some(gtx) => rvb.addInt(gtx)
          case None => rvb.setMissing()
        }

        val newAD = Genotype.ad(g).map(downcodeAD)
        newAD match {
          case Some(adx) =>
            rvb.startArray(adx.length)
            adx.foreach { adxi => rvb.addInt(adxi) }
            rvb.endArray()
          case None => rvb.setMissing()
        }

        Genotype.dp(g) match {
          case Some(dpx) => rvb.addInt(dpx)
          case None => rvb.setMissing()
        }

        val newGQ = newPX.map(Genotype.gqFromPL)
        newGQ match {
          case Some(gqx) => rvb.addInt(gqx)
          case None => rvb.setMissing()
        }

        newPX match {
          case Some(pxx) =>
            rvb.startArray(pxx.length)
            pxx.foreach { pxxi => rvb.addInt(pxxi) }
            rvb.endArray()
          case None => rvb.setMissing()
        }

        rvb.addBoolean(g._fakeRef)
        rvb.addBoolean(g._isLinearScale)

        rvb.endStruct()
      }

      def subsetPX(px: Array[Int]): Array[Int] = {
        val (newPx, minP) = px.zipWithIndex
          .filter {
            case (p, i) =>
              val gTPair = Genotype.gtPair(i)
              (gTPair.j == 0 || oldToNew(gTPair.j) != 0) && (gTPair.k == 0 || oldToNew(gTPair.k) != 0)
          }
          .foldLeft((Array.fill(triangle(newCount))(0), Int.MaxValue)) {
            case ((newPx, minP), (p, i)) =>
              newPx(downcodeGT(i)) = p
              (newPx, min(p, minP))
          }

        newPx.map(_ - minP)
      }

      def subsetGenotype(g: Genotype) {
        val newPX = Genotype.px(g).map(subsetPX)

        var newGT = newPX.map(_.zipWithIndex.min._2)
        if (filterAlteredGenotypes && newGT != Genotype.gt(g))
          newGT = None

        rvb.startStruct()
        newGT match {
          case Some(gtx) => rvb.addInt(gtx)
          case None => rvb.setMissing()
        }

        val newAD = Genotype.ad(g).map(_.zipWithIndex.filter({ case (d, i) => i == 0 || oldToNew(i) != 0 }).map(_._1))
        newAD match {
          case Some(adx) =>
            rvb.startArray(adx.length)
            adx.foreach { adxi => rvb.addInt(adxi) }
            rvb.endArray()
          case None => rvb.setMissing()
        }

        Genotype.dp(g) match {
          case Some(dpx) => rvb.addInt(dpx)
          case None => rvb.setMissing()
        }

        val newGQ = newPX.map(Genotype.gqFromPL)
        newGQ match {
          case Some(gqx) => rvb.addInt(gqx)
          case None => rvb.setMissing()
        }

        newPX match {
          case Some(pxx) =>
            rvb.startArray(pxx.length)
            pxx.foreach { pxxi => rvb.addInt(pxxi) }
            rvb.endArray()
          case None => rvb.setMissing()
        }

        rvb.addBoolean(g._fakeRef)
        rvb.addBoolean(g._isLinearScale)

        rvb.endStruct()
      }

      rvb.startArray(localNSamples)
      gs.foreach { g =>
        if (g == null)
          rvb.setMissing()
        else {
          if (subset)
            subsetGenotype(g)
          else
            downcodeGenotype(g)
        }
      }
      rvb.endArray()
    }

    def filter(rdd: RDD[RegionValue],
      filterMinrepped: Boolean, filterMoving: Boolean, verifyMinrepped: Boolean): RDD[RegionValue] = {

      val localRowType = vds.matrixType.rowType
      val newRowType = newMatrixType.rowType

      rdd.mapPartitions { it =>
        var prevLocus: Locus = null

        it.flatMap { rv =>
          val rvb = new RegionValueBuilder()
          val rv2 = RegionValue()

          val ur = new UnsafeRow(localRowType, rv.region, rv.offset)

          val v = ur.getAs[Variant](1)
          val va = ur.get(2)
          val gs = ur.getAs[IndexedSeq[Genotype]](3)

          filterAllelesInVariant(v, va)
            .filter { case (newV, _, _) =>
              val moving = (prevLocus != null && prevLocus == v.locus) || (newV.locus != v.locus)

              if (moving && verifyMinrepped)
                fatal(s"found non-minrepped variant $v")

              var keep = true
              if (moving) {
                if (filterMoving)
                  keep = false
              } else {
                if (filterMinrepped)
                  keep = false
              }

              if (!keepStar && newV.isBiallelic && newV.altAllele.isStar)
                keep = false

              println(moving, filterMoving, filterMinrepped, keep)

              keep
            }
            .map { case (newV, newToOld, oldToNew) =>
              rvb.set(rv.region)
              rvb.start(newRowType)
              rvb.startStruct()
              rvb.addAnnotation(newRowType.fieldType(0), newV.locus)
              rvb.addAnnotation(newRowType.fieldType(1), newV)

              val newVA = updateAnnotation(v, va, newToOld)
              rvb.addAnnotation(newVAType, newVA)

              if (newV == v)
                rvb.addAnnotation(newRowType.fieldType(3), gs)
              else
                updateGenotypes(rvb, gs, oldToNew, newToOld.length)
              rvb.endStruct()

              prevLocus = newV.locus

              rv2.set(rv.region, rvb.end())
              rv2
            }
        }
      }
    }

    val newRDD2: OrderedRDD2 =
      if (minrepped) {
        OrderedRDD2(newMatrixType.orderedRDD2Type,
          vds.rdd2.orderedPartitioner,
          filter(vds.rdd2, filterMinrepped = false, filterMoving = false, verifyMinrepped = true))
      } else {
        val minrepped = OrderedRDD2(newMatrixType.orderedRDD2Type,
          vds.rdd2.orderedPartitioner,
          filter(vds.rdd2, filterMinrepped = false, filterMoving = true, verifyMinrepped = false))

        val moving = OrderedRDD2.shuffle(newMatrixType.orderedRDD2Type,
          vds.rdd2.orderedPartitioner,
          filter(vds.rdd2, filterMinrepped = true, filterMoving = false, verifyMinrepped = false))

        minrepped.partitionSortedUnion(moving)
      }

    vds.copy2(rdd2 = newRDD2, vaSignature = newVAType)
  }
}
