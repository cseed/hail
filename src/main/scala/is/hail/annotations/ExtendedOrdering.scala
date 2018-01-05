package is.hail.annotations

import org.apache.spark.sql.Row

object ExtendedOrdering {
  def extendToNull[T](ord: Ordering[T]): ExtendedOrdering[T] = new ExtendedOrdering[T] {
    def compareNonnull(x: T, y: T, missingGreatest: Boolean): Int = ord.compare(x, y)
  }

  def iterableOrdering[T](ord: ExtendedOrdering[T]): ExtendedOrdering[Iterable[T]] =
    new ExtendedOrdering[Iterable[T]] {
      def compareNonnull(x: Iterable[T], y: Iterable[T], missingGreatest: Boolean): Int = {
        val xit = x.iterator
        val yit = y.iterator

        while (xit.hasNext && yit.hasNext) {
          val c = ord.compare(xit.next(), yit.next(), missingGreatest)
          if (c != 0)
            return c
        }

        java.lang.Boolean.compare(xit.hasNext, yit.hasNext)
      }
    }

  def sortArrayOrdering(ord: ExtendedOrdering[Annotation]): ExtendedOrdering[Array[Annotation]] =
    new ExtendedOrdering[Array[Annotation]] {
      private val itOrd = iterableOrdering(ord)

      def compareNonnull(x: Array[Annotation], y: Array[Annotation], missingGreatest: Boolean): Int = {
        val scalaOrd = ord.toOrdering(missingGreatest)
        itOrd.compareNonnull(x.sorted(scalaOrd), y.sorted(scalaOrd), missingGreatest)
      }
    }

  def setOrdering(ord: ExtendedOrdering[Annotation]): ExtendedOrdering[Iterable[Annotation]] =
    new ExtendedOrdering[Iterable[Annotation]] {
      private val saOrd = sortArrayOrdering(ord)

      def compareNonnull(x: Iterable[Annotation], y: Iterable[Annotation], missingGreatest: Boolean): Int = {
        val scalaOrd = ord.toOrdering(missingGreatest)
        saOrd.compareNonnull(x.toArray.sorted(scalaOrd), y.toArray.sorted(scalaOrd), missingGreatest)
      }
    }

  def mapOrdering[K, V](ord: ExtendedOrdering[Annotation]): ExtendedOrdering[Map[K, V]] =
    new ExtendedOrdering[Map[K, V]] {
      private val saOrd = sortArrayOrdering(ord)

      def compareNonnull(x: Map[K, V], y: Map[K, V], missingGreatest: Boolean): Int = {
        val scalaOrd = ord.toOrdering(missingGreatest)
        saOrd.compareNonnull(x.toArray.map { case (k, v) => Row(k, v): Annotation },
          x.toArray.map { case (k, v) => Row(k, v): Annotation },
          missingGreatest)
      }
    }

  def rowOrdering(fieldOrd: Array[ExtendedOrdering[Annotation]]): ExtendedOrdering[Row] =
    new ExtendedOrdering[Row] {
      def compareNonnull(x: Row, y: Row, missingGreatest: Boolean): Int = {
        var i = 0
        while (i < fieldOrd.length) {
          val c = fieldOrd(i).compare(x.get(i), y.get(i), missingGreatest)
          if (c != 0)
            return c
          i += 1
        }

        // equal
        0
      }
    }
}

abstract class ExtendedOrdering[T] extends Serializable {
  outer =>

  def compareNonnull(x: T, y: T, missingGreatest: Boolean): Int

  def compare(x: T, y: T, missingGreatest: Boolean): Int = {
    if (y == null) {
      if (x == null)
        0
      else if (missingGreatest) -1 else 1
    } else {
      if (x == null)
        if (missingGreatest) 1 else -1
      else
        compareNonnull(x, y, missingGreatest)
    }
  }

  // reverses the sense of the non-null comparison only
  def reverse: ExtendedOrdering[T] = new ExtendedOrdering[T] {
    override def reverse: ExtendedOrdering[T] = outer

    def compareNonnull(x: T, y: T, missingGreatest: Boolean): Int = outer.compareNonnull(y, x, missingGreatest)
  }

  def compare(x: T, y: T): Int = compare(x, y, missingGreatest = true)

  def lt(x: T, y: T): Boolean = compare(x, y) < 0

  def lteq(x: T, y: T): Boolean = compare(x, y) <= 0

  def gt(x: T, y: T): Boolean = compare(x, y) > 0

  def gteq(x: T, y: T): Boolean = compare(x, y) >= 0

  def equiv(x: T, y: T): Boolean = compare(x, y) == 0

  def nequiv(x: T, y: T): Boolean = compare(x, y) != 0

  def min(x: T, y: T): T = if (lt(x, y)) x else y

  def max(x: T, y: T): T = if (lt(x, y)) y else x

  def toOrdering: Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = outer.compare(x, y)
  }

  def toOrdering(missingGreatest: Boolean): Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = outer.compare(x, y, missingGreatest)
  }

  def annotationOrdering: ExtendedOrdering[Annotation] = new ExtendedOrdering[Annotation] {
    def compareNonnull(x: Annotation, y: Annotation, missingGreatest: Boolean): Int =
      outer.compareNonnull(x.asInstanceOf[T], y.asInstanceOf[T], missingGreatest)
  }
}
