---
title: "Updating Spark Table Metadata in append mode"
layout: single
classes: wide
tags: [spark, scala]

excerpt: "This post is about one of the spark features with which you can modify the metadata of a table."

published: true
comments: true
---

Spark supports a feature which adds metadata information to spark table. Metadata can be your Number, a String or an Array type that can be used to store table specific stats or data aggregation related info. 
for example, how many classes you have in your feature column?
or what is the maximum date in your date column?

These type of details once persisted can be used for further calculation whenever you are going to use these tables in near future.

Let's see the above case through an example:

```scala
val data = Seq((12, 20180411), (5, 20180411), (11, 20180411), (2, 20180412), (29, 20180413),
    (31, 20180414), (18, 20180415), (2, 20180412), (31, 20180413), (8, 20180416), (29, 20180413),
    (31, 20180414), (8, 20180415), (2, 20180412), (23, 20180413), (51, 20180414), (15, 20180415))
    
val orders = data.toDF("order id", "date")
val maxDate = orders.agg(max("date")).as[Int].take(1)(0)

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
val metadata: Metadata = new MetadataBuilder().putLong("max_dt", maxDate).build

orders.groupBy('date as("date", metadata)).agg(count("order id") as "order_count")
  .write.saveAsTable("daily_order_count")

val tillProcessDate = sparkSession.read.table("daily_order_count")
  .schema("date").metadata.getLong("max_dt")
```

So in the above example you can see that I've created a dataset first and then added the maxDate after aggregation to the metadata instance.
This metadata will be attached with the schema of the table I'm writing. Further we can read the table metadata from the table schema and use it for our processing. 
This way we'll be able to persist table details in our schema.

In the below example you can see, I've updated the metadata information:

```scala
val moreOrders = (data ++ Seq((2, 20180417), (41, 20180417), (25, 20180417),
  (41, 20180418), (25, 20180418))).toDF("order id", "date")

val maxDate = moreOrders.agg(max("date")).as[Int].take(1)(0)

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
val metadata: Metadata = new MetadataBuilder().putLong("max_dt", maxDate).build

moreOrders.groupBy('date as("date", metadata)).agg(count("order id") as "order_count")
  .write.mode("overwrite").saveAsTable("daily_order_count")

val tillProcessDate = spark.read.table("daily_order_count")
  .schema("date").metadata.getLong("max_dt")
```

So you must've noticed that I used **overwrite** mode to update my table. This will update my data
 as well as my schema according to the details I specify. 

But what about the append mode? 
There could also be a case where you don't want to drop the data but just want to new data alongwith new table metadata details.
So in this case we have a limitation in spark, as it doesn't support this feature in append mode. 
If you want to update the metadata, instead of updating it spark will keep the old values.

So here is the solution to update the metadata in append mode in spark 2.2 version.

```scala
package org.apache.spark.sql

import scala.language.implicitConversions
import org.apache.spark.sql.types.StructType

object UpdateSparkMetadata {
  def alterTableSchema(_table: String, schema: StructType)(implicit spark: SparkSession): Unit = {
    spark.sessionState.catalog.alterTableSchema(
      spark.sessionState.sqlParser.parseTableIdentifier(_table), schema
    )
  }
}
```

So in above example, I simply created an object using **org.apache.spark.sql** package.
Note that this package includes a developer API called **sessionState**. In this package we have **SessionCatalag** object which provides us a method called **alterTableSchema**, which accepts **TableIdentifier** and **Schema** as it's method parameters.

```scala
val moreOrders = (data ++ Seq((2, 20180417), (41, 20180417), (25, 20180417),
  (41, 20180418), (25, 20180418))).toDF("order id", "date")

val maxDate = moreOrders.agg(max("date")).as[Int].take(1)(0)

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
val metadata: Metadata = new MetadataBuilder().putLong("max_dt", maxDate).build

val orderAgg = moreOrders.groupBy('date as("date", metadata)).agg(count("order id") as "order_count")
orderAgg.write.mode("overwrite").saveAsTable("daily_order_count")

val tillProcessDate = spark.read.table("daily_order_count")
  .schema("date").metadata.getLong("max_dt")
  org.apache.spark.sql.UpdateSparkMetadata.alterTableSchema("daily_order_count", orderAgg.schema)
```

So once you call the method and pass the required argumeents it will forcefully update the metadata information in the table.

Hope you find article useful, for complete code please follow my github [link](www.github
.com/skp33/update-spark-table-metadata)

