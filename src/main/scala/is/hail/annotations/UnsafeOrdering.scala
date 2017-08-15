package is.hail.annotations

abstract class UnsafeOrdering {
  def compare(r1: MemoryBuffer, o1: Int, r2: MemoryBuffer, o2: Int): Int
}
