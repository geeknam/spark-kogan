package com.kogan.api.products

import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import com.kogan.api.util.Slug.StringToSlug
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import scala.util.parsing.json.JSON


class ProductPartitionReader(schema: StructType, productFilter: Map[String, String],
                             pushedFilters: Array[Filter])
  extends InputPartitionReader[InternalRow] {

  val PRODUCT_API_URL = "https://www.kogan.com/api/v1/products/"
  val PRICE_PARAM_KEY = "price"
  val fieldNames = schema.map(f => f.name)

  var index = 0

  lazy val products = getResponse()
    .map(row => row.filterKeys(key => fieldNames.contains(key)))

  override def next(): Boolean = index < products.length

  override def get(): InternalRow = {
    // Use the order of field names in the schema
    // and get the Seq of values for products response
    val row = InternalRow.fromSeq(
      fieldNames
        .map(fn => products(index).get(fn).orNull match {
          case value: String => UTF8String.fromString(value)
          case value: Any => value
        })
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

  private def getRangeFilters(key: String) = {
    pushedFilters
      .filter(f => f match {
        case GreaterThan(attribute, _) if(attribute == key) => true
        case GreaterThanOrEqual(attribute, _) if(attribute == key) => true
        case LessThan(attribute, _) if(attribute == key) => true
        case LessThanOrEqual(attribute, _) if(attribute == key) => true
        case _ => false
      })
  }

  private def getPriceFilterMap = {
    getRangeFilters(PRICE_PARAM_KEY) match {
      case filters if filters.length == 0 => Map()
      case filters if filters.length == 1 => filters(0) match {
        case filter: GreaterThan => Map(filter.attribute -> filter.value.toString)
        case filter: LessThan => Map(filter.attribute -> s",${filter.value}")
      }
      case filters if filters.length == 2 => (filters(0), filters(1)) match {
        case (first: GreaterThan, second: LessThan) => Map(first.attribute -> s"${first.value},${second.value}")
        case (first: LessThan, second: GreaterThan) => Map(first.attribute -> s"${second.value},${first.value}")
      }
      case _ => Map()
    }
  }

  private def getPushDowProductFilters(): Map[String, String] = {

    val brandFilterMap = equalToFilter("brand", key => key,value => value.slug)

    val shippingFilterMap = equalToFilter(
      "free_shipping", _ => "shipping",
      value => value match {
        case "true" => "free"
        case "false" => "not-free"
      }
    )

    val dispatchFilterMap = equalToFilter(
      "fast_dispatch", _ => "dispatch",
      value => value match {
        case "true" => "fast"
        case "false" => "not-fast"
      }
    )

    getPriceFilterMap ++ brandFilterMap ++ shippingFilterMap ++ dispatchFilterMap
  }

  private def getResponse() = {
    val params = productFilter ++ getPushDowProductFilters()
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