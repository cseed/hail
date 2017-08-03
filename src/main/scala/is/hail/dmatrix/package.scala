package is.hail

import is.hail.annotations.Annotation
import is.hail.sparkextras.OrderedRDD

package object dmatrix {
  // FIXME the key should never be explicit
  // FIXME we should have OrderedRDD[T] extends RDD[T]
  type DMatrixRowRDD = OrderedRDD[Value, Value, Value]
}
