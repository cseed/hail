package is.hail.utils.richUtils

import is.hail.utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SQLContext}

class RichSQLContext(val sqlContext: SQLContext) extends AnyVal {
  def readParquetSorted(dirname: String, selection: Option[Array[String]] = None): RDD[Row] = {
    // FIXME
    /*
    val parquetFiles = sqlContext.sparkContext.hadoopConfiguration.globAll(Array(dirname + "*.parquet"))
    if (parquetFiles.isEmpty)
      return sqlContext.sparkContext.emptyRDD[Row] */

    var df = sqlContext.read.parquet(dirname)
    selection.foreach { cols =>
      df = df.select(cols(0), cols.tail: _*)
    }

    val rdd = df.rdd
    println("read partitions", rdd.partitions.length)

    val oldIndices = rdd.partitions
      .map { p => getParquetPartNumber(partitionPath(p)) }
      .zipWithIndex
      .sortBy(_._1)
      .map(_._2)

    rdd.reorderPartitions(oldIndices)
  }
}
