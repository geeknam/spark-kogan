package com.kogan.api.products

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, LessThan}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType
import com.kogan.api.util.Slug.StringToSlug

import scala.util.parsing.json.JSON

class ProductApiReaderFactory(schema: StructType, productFilter: Map[String, String],
                              pushedFilters: Array[Filter])
  extends DataReaderFactory[Row] with DataReader[Row] {

  val PRODUCT_API_URL = "https://www.kogan.com/api/v1/products/"
  val PRICE_PARAM_KEY = "price"
  val fieldNames = schema.map(f => f.name)

  override def createDataReader(): DataReader[Row] = new ProductApiReaderFactory(
    schema, productFilter, pushedFilters
  )

  var index = 0

  lazy val products = getResponse()
    .map(row => row.filterKeys(key => fieldNames.contains(key)))

  override def next(): Boolean = index < products.length

  override def get(): Row = {
    // Use the order of field names in the schema
    // and get the Seq of values for products response
    val row = Row.fromSeq(
      fieldNames.map(fn => products(index).get(fn).orNull)
    )
    index = index + 1
    row
  }

  override def close(): Unit = {}

  private def equalToFilter(key: String, keyMapFunc: (String) => String,
                            valueMapFunc: (String) => String) = {
    val filters = pushedFilters
      .filter(f => f match {
        case EqualTo(attribute, _) if(attribute == key) => true
        case _ => false
      })

    filters.length match {
      case 0 => Map()
      case _ => {
        val filter = filters(0).asInstanceOf[EqualTo]
        Map(keyMapFunc(filter.attribute) -> valueMapFunc(filter.value.toString))
      }
    }
  }

  private def updateProductFilter(): Map[String, String] = {
    val priceFilter = pushedFilters
      .filter(f => f match {
        case GreaterThan(attribute, _) if(attribute == PRICE_PARAM_KEY) => true
        case LessThan(attribute, _) if(attribute == PRICE_PARAM_KEY) => true
        case _ => false
      })

    val priceFilterMap = priceFilter.length match {
      case 0 => Map()
      case 1 => priceFilter(0) match {
        case filter: GreaterThan => Map(filter.attribute -> filter.value.toString)
        case filter: LessThan => Map(filter.attribute -> s",${filter.value}")
      }
      case 2 => (priceFilter(0), priceFilter(1)) match {
        case (first: GreaterThan, second: LessThan) => Map(first.attribute -> s"${first.value},${second.value}")
        case (first: LessThan, second: GreaterThan) => Map(first.attribute -> s"${second.value},${first.value}")
      }
      case _ => Map()
    }

    val brandFilterMap = equalToFilter("brand", key => key,value => value.slug)
    
    val shippingFilterMap = equalToFilter(
      "free_shipping", key => "shipping",
      value => value match {
        case "true" => "free"
        case "false" => "not-free"
      }
    )

    val dispatchFilterMap = equalToFilter(
      "fast_dispatch", key => "dispatch",
      value => value match {
        case "true" => "fast"
        case "false" => "not-fast"
      }
    )

    priceFilterMap ++ brandFilterMap ++ shippingFilterMap ++ dispatchFilterMap
  }

  private def getResponse() = {
    val params = productFilter ++ updateProductFilter()
    val response = requests.get(
      PRODUCT_API_URL,
      params = params,
      verifySslCerts = false
    )
    JSON.parseFull(response.text()) match {
      case Some(resp) => resp.asInstanceOf[
          Map[String, List[Map[String, Any]]]
        ]("objects")
      case _ => List()
    }
  }
}