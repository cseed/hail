package is.hail

import is.hail.annotations.Annotation
import is.hail.sparkextras.OrderedRDD

package object dmatrix {
  type RowRDD = OrderedRDD[Annotation, Annotation, Annotation]
}
