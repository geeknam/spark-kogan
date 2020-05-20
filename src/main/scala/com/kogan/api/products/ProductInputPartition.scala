package com.kogan.api.products

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class ProductInputPartition(schema: StructType,
                            filter: Map[String, String],
                            pushedFilters: Array[Filter])
  extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new ProductPartitionReader(schema, filter, pushedFilters)
}
