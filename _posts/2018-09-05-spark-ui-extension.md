---
title: "How to extend Spark UI"
layout: single
classes: wide
tags: [spark, scala]

excerpt: "In this post, I'll be adding a new tab in Spark UI to show custom details about any spark application"

published: true
comments: true
---

As you know spark is a framework with lots of unique and developer friendly features,  one of them
is its UI(Spark UI). It provides variety of information about spark, like currently running jobs,
their stages, tasks, memory usage and plenty others.

But if you are developing an application with Spark, at times you would want to show some other
details. There is no point to create a separate UI module to show your basic details or metrics.

Spark is already providing you that functionality using which you can add details and visualize
metrics within the Spark UI.

Let's take an example, In which I want to show schema information of my Spark dataframes in Spark UI
So in that case you need three things:
- first is, data object to store the details required to show in UI which can be updated with new information based on the requirement.

    ```scala
    object Utility {
      val allSchema: ConcurrentMap[String, String] = new ConcurrentHashMap[String, String]()
    
      implicit class DataFrameSchema[T](df: Dataset[T]) {
        def registerSchema: Dataset[T] = {
            schemas.put(Thread.currentThread().getStackTrace.slice(2,4).mkString("\n"), df.schema
              .treeString)
          df
        }
      }
    }
    ```

    Here __allSchema__ map stores your schema information and __regiserSchema__ function updates schema for it.

- Second is, to create a class which extends __WebUIPage__, in which you can write your HTML logic
for your visualization

```scala
class DataFrameSchemaUIPage(parent: ExtendedUIServer) extends WebUIPage("") with Logging {

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    import scala.collection.JavaConversions._

    val content = <h4>The below table shows registered dataframes on the left, with there schemas on the
        right.</h4>
        <br/>
      <div>
        <table class="table table-bordered table-condensed" id="task-summary-table">
          <thead>
            <tr style="background-color: rgb(255, 255, 255);">
              <th width="50%" class="">DataFrame</th>
              <th width="50%" class="">Schema</th>
            </tr>
          </thead>
          <tbody>
            {Utility.schemas.map(x =>
            <tr style="background-color: rgb(249, 249, 249);">
              <td>{s"${x._1}"}</td>
              <td><pre>{s"${x._2}"}</pre></td>
            </tr>)}
          </tbody>
          <tfoot></tfoot>
        </table>
      </div>

    UIUtils.headerSparkPage(
      "This is the extension to Spark UI to display custom information about your application.",
      content, parent)
  }
}
```

This class is having all the logic for rendering your html page.

- Third is, to attach your page (class which having html logic) with existing spark UI

```scala
class ExtendedUIServer(sparkContext: SparkContext)
  extends SparkUITab(getSparkUI(sparkContext), "dataframeschema")
    with Logging {

  override val name = "Dataframe Schema"

  val parent: SparkUI = getSparkUI(sparkContext)

  attachPage(new DataFrameSchemaUIPage(this))
  parent.attachTab(this)

  def detach() {
    getSparkUI(sparkContext).detachTab(this)
  }
}

object ExtendedUIServer {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
```

Here I'm attaching my page using this method call __attachPage(new DataFrameSchemaUIPage(this))__


So that is it, now you are ready to go and test your custom webpage.

```scala
object TestUIExtension {
  def main(args: Array[String]): Unit = {
    val spark = ...

    new ExtendedUIServer(spark.sparkContext)

    import spark.implicits._
    import Utility._
    Seq("1").toDF("id").registerSchema
    println("First Dataframe")
    Thread.sleep(10000)

    import org.apache.spark.sql.functions.count
    Seq(("1", 1)).toDF("id", "count").groupBy("id").agg(count("id") as "count")
      .registerSchema
    println("Second Dataframe")

    Seq("1").toDF("otherId").distinct().registerSchema.show
    println("Third Dataframe")
    Thread.sleep(60000)
    println("Test Done ..")
  }
}
```

So as a first step I'll create an instance of __ExtendedUIServer__ class which will attach and 
render your page and then later I will call __registerSchema__ which will add schema of the dataframe to UI page.

For full code you can visit my github repo [link](https://github.com/skp33/spark-ui-extension).