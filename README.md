## Spark DataSourceV2 for Kogan.com API

Spark version 2.3.0

### Usage

```scala
val df = spark.read
  .format("com.kogan.api.products")
  // Used for filtering department & category
  //.option("department", "phones")
  //.option("category", "led-tv")
  .option("store", "au")
  .option("group_variants", "false")
  .option("max", "200")
  .schema(
    "title STRING, sku STRING, slug STRING, brand STRING, category STRING, " +
      "stock STRING, on_sale BOOLEAN, price DOUBLE, free_shipping BOOLEAN," +
      "fast_dispatch BOOLEAN"
  )
  .load()

// Supports the following predicate pushdown
// - price
// - brand
// - free_shipping
// - fast_dispatch
df.filter(
    "price < 2000 AND brand = 'Apple'" +
      " AND free_shipping = true AND fast_dispatch = true"
  )
  .show


```
