---
title: "Evaluating column expressions without a dataframe in apache spark"
layout: single
classes: wide
tags: [spark, scala]

excerpt: "This post is about one of the spark feature which evaluate column expressions without a dataframe"

published: true
comments: true
---
Spark these days is a de-facto for ETL and building data pipelines. During the pipeline building phase we generally come across at writing test cases for the code. 
There will be many incidents when we write complex column expressions or UDFs and a need to test them arises. 

So I'll be discussing about how I used "eval()" method to test my expressions and UDFs without even creating a dataframe or running a spark application.
let me explain this with a few simple examples:

```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions.split
def checkDevice(device: Column) = array_contains(split(device, ","), "mobile")

// Exiting paste mode, now interpreting.

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions.split
checkDevice: (device: org.apache.spark.sql.Column)org.apache.spark.sql.Column

scala> checkDevice(lit("mobile,desktop")).expr.eval()
res3: Any = true

scala> checkDevice(lit("mobiles,desktop")).expr.eval()
res4: Any = false
```


Here is another example in which i am evaluating a udf:

```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)

import org.apache.spark.sql.functions.udf
val cleanColumn = udf{ (str: String) => {
    str.toLowerCase.replaceAll("\\W", " ").replaceAll("\\s+", " ").trim.split(" ").filter(w => w.size > 2).distinct
}}

// Exiting paste mode, now interpreting.

import org.apache.spark.sql.functions.udf
cleanColumn: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,ArrayType(StringType,true),Some(List(StringType)))

scala> cleanColumn(lit("   here is a test string ... ")).expr.eval()
res5: Any = [here,test,string]
```

So what I've tried to show here is that you can evaluate your column expression or UDF without using a dataframe. This could be useful when you are doing test driven developement and want to test your changes more frequently.
