package com.kogan.api.products

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.StructType

class DefaultSource extends DataSourceV2 with ReadSupport {

  val ALLOWED_FILTERS = Seq("store", "category", "department", "group_variants")

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    new ProductApiReader(
      schema, getFilters(options), options
    )
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new ProductApiReader(
      new StructType(), getFilters(options), options
    )
  }

  private def getFilters(options: DataSourceOptions) = {
    // Allow specifying .option() to filter products
    // Use default limit=200 in API response
    val defaultFilters = Map(
      "limit" -> DefaultSource.DEFAULT_LIMIT.toString
    )
    ALLOWED_FILTERS
      .filter(key => options.get(key).isPresent)
      .map(key => key -> options.get(key).orElse(""))
      .toMap ++ defaultFilters
  }

}

object DefaultSource {
  val DEFAULT_LIMIT = 200
}
