package com.kogan.api.products

import java.util

import com.kogan.api.products.ProductApiReader.DEFAULT_MAX
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters}
import org.apache.spark.sql.types.StructType

class ProductApiReader(schema: StructType, val productFilter: Map[String, String],
                       options: DataSourceOptions)
  extends DataSourceReader with SupportsPushDownFilters {

  val maxItems = options.getInt("max", DEFAULT_MAX)

  val numPartitions = maxItems / DefaultSource.DEFAULT_LIMIT

  private def getOffset(page: Int) = {
    page match {
      case 0 => 0
      case _ => (DefaultSource.DEFAULT_LIMIT * page + 1)
    }
  }

  private def getPages(numPartitions: Int) = List.range(0, numPartitions)

  override def readSchema(): StructType = schema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val factoryList = new util.ArrayList[DataReaderFactory[Row]]
    getPages(numPartitions).foreach(page => {
      val filter = productFilter ++ Map(
        ProductApiReader.OFFSET_PARAM_KEY -> getOffset(page).toString
      )
      
      factoryList.add(
        new ProductApiReaderFactory(schema, filter, pushedFilters)
      )
    })

    factoryList
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushedFilters = filters
    pushedFilters
  }

  var pushedFilters: Array[Filter] = Array[Filter]()
}


object ProductApiReader {
  val DEFAULT_MAX = 1000
  val OFFSET_PARAM_KEY = "offset"
}