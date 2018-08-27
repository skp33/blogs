---
title: "Integrating Rest services with Apache Spark without any Web-Server"
layout: single
classes: wide
tags: [spark, scala]

excerpt: "This post is about enabling basic Rest API in your Spark application with the minimal code"

published: true
comments: true
---

There is always a use case in which you have to use web-service and enable user intervention to achieve some task. 
But how about with spark?
In spark there could be a situation where you have to perform some tasks dynamically: like trigger a spark job, change config parameter or any application 
specific tasks.

In order to achieve this, we need to integrate rest framework like jersey which provides API endpoints to those end services. 

But have you noticed that when you start a spark app, it opens a port for UI.
This UI port by default is __4040__ which also provide you few Rest API to get more information about your application, like number of jobs, stages, tasks, environment variables etc. 
For more information you can go through this link [Spark Monitoring](https://spark.apache.org/docs/latest/monitoring.html#rest-api)

So why not reuse this existing spark feature and add our rest apis to achieve some basic requirements.

if you see below code of [__getServletHandler__](https://github.com/apache/spark/blob/v2.2.0/core/src/main/scala/org/apache/spark/status/api/v1/ApiRootResource.scala#L238)
 method of __org.apache.spark.status.api.v1.ApiRootResource__ class 

```scala
  def getServletHandler(uiRoot: UIRoot): ServletContextHandler = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/api")
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.spark.status.api.v1")
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
```

spark provides __org.apache.spark.status.api.v1__ package to jersey rest. This means, if you implement any rest api within this package it will automaticlly register your API with jersey.


Cool, lets write some rest api.
```scala
package org.apache.spark.status.api.v1

import javax.ws.rs.{GET, Path, PathParam, Produces}
import javax.ws.rs.core.MediaType

@Path("/custom")
class TestService extends {
  @GET
  @Path("sum/{x}/{y}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getApplicationList(
                          @PathParam("x") x: Int,
                          @PathParam("y") y: Int): Int = {
    x+y
  }
}
```

Now call this api once your spark context is up using this [http://localhost:4040/api/custom/sum/43/8897] endpoint.

So far everything seems fine, but what if you want to provide your own package to create rest services. 

There's a workaround for this as well, check the below code:


```scala
package org.apache.spark.rest

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.ui.SparkUI
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

class RestAPI {
  def getServletHandler: ServletContextHandler = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/rest")

    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.spark.rest.services")

    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}

object RestAPI extends Logging {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }

  def apply(sparkContext: SparkContext): Unit = attach(sparkContext)

  def attach(sparkContext: SparkContext): Unit = {
    getSparkUI(sparkContext).attachHandler(new RestAPI().getServletHandler)
    logInfo("Started rest-server")
  }

  def detach(sparkContext: SparkContext): Unit = {
    getSparkUI(sparkContext).detachHandler(new RestAPI().getServletHandler)
    logInfo("Stopped rest-server")
  }
}
```

After that you will have to call __attach(sparkContext: SparkContext)__ method in your code whenever you want to start your API.
This way you can write your basic rest features and integrate them with your spark application.

For full code you can visit my github repo [link](https://github.com/skp33/spark-rest).