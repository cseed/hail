package is.hail.rvd

import is.hail.annotations.{MemoryBuffer, RegionValue, RegionValueBuilder}
import is.hail.expr.Type
import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.rdd.{AggregateWithContext, RDD}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag


object RVD {
  def apply(rowType: Type, rdd: RDD[RegionValue]): ConcreteRVD = new ConcreteRVD(rowType, rdd)
}

case class PersistedRVRDD(
  persistedRDD: RDD[RegionValue],
  iterationRDD: RDD[RegionValue])

trait RVD {
  self =>
  def rowType: Type

  def rdd: RDD[RegionValue]

  def sparkContext: SparkContext = rdd.sparkContext

  def getNumPartitions: Int = rdd.getNumPartitions

  def partitions: Array[Partition] = rdd.partitions

  def map[T](f: (RegionValue) => T)(implicit tct: ClassTag[T]): RDD[T] = rdd.map(f)

  def mapPartitions[T](f: (Iterator[RegionValue]) => Iterator[T])(implicit tct: ClassTag[T]): RDD[T] = rdd.mapPartitions(f)

  def mapPartitionsWithIndex[T](f: (Int, Iterator[RegionValue]) => Iterator[T])(implicit tct: ClassTag[T]): RDD[T] = rdd.mapPartitionsWithIndex(f)

  def map(newRowType: Type)(f: (RegionValue) => RegionValue): RVD = RVD(newRowType, rdd.map(f))

  def treeAggregate[U: ClassTag](zeroValue: U)(
    seqOp: (U, RegionValue) => U,
    combOp: (U, U) => U,
    depth: Int = 2): U = rdd.treeAggregate(zeroValue)(seqOp, combOp, depth)

  def aggregateWithContext[U: ClassTag, V](context: () => V)(zeroValue: U)
    (seqOp: (V, U, RegionValue) => U, combOp: (U, U) => U): U = {
    AggregateWithContext.aggregateWithContext(rdd)(context)(zeroValue)(seqOp, combOp)
  }

  def mapWithContext[C](newRowType: Type)(makeContext: () => C)(f: (C, RegionValue) => RegionValue) =
    RVD(newRowType, rdd.mapPartitions { it =>
      val c = makeContext()
      it.map { rv => f(c, rv) }
    })

  def mapPartitions(newRowType: Type)(f: (Iterator[RegionValue]) => Iterator[RegionValue]): RVD = RVD(newRowType, rdd.mapPartitions(f))

  def filter(f: (RegionValue) => Boolean): RVD = RVD(rowType, rdd.filter(f))

  def count(): Long = rdd.count()

  protected def persistRVRDD(level: StorageLevel): PersistedRVRDD = {
    val localRowType = rowType

    // copy, persist region values
    val persistedRDD = rdd.mapPartitions { it =>
      val region = MemoryBuffer()
      val rvb = new RegionValueBuilder(region)
      it.map { rv =>
        region.clear()
        rvb.start(localRowType)
        rvb.addRegionValue(localRowType, rv)
        val off = rvb.end()
        RegionValue(region.copy(), off)
      }
    }
      .persist(level)

    PersistedRVRDD(persistedRDD,
      persistedRDD
        .mapPartitions { it =>
          val region = MemoryBuffer()
          val rv2 = RegionValue(region)
          it.map { rv =>
            region.setFrom(rv.region)
            rv2.setOffset(rv.offset)
            rv2
          }
        })
  }

  def storageLevel: StorageLevel = StorageLevel.NONE

  def persist(level: StorageLevel): RVD = {
    val PersistedRVRDD(persistedRDD, iterationRDD) = persistRVRDD(level)
    new RVD {
      val rowType: Type = self.rowType

      val rdd: RDD[RegionValue] = iterationRDD

      override def storageLevel: StorageLevel = level

      override def persist(newLevel: StorageLevel): RVD = {
        if (newLevel == level)
          this
        else
          throw new IllegalArgumentException("already persisted")
      }

      override def unpersist(): RVD = {
        persistedRDD.unpersist()
        self
      }
    }
  }

  def unpersist(): RVD = throw new IllegalArgumentException("not persisted")

  def coalesce(maxPartitions: Int, shuffle: Boolean): RVD = RVD(rowType, rdd.coalesce(maxPartitions, shuffle = shuffle))
}
