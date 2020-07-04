import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._

object dashboard extends MainWithSpark {

  val getDomain2FromUrl = udf{ url: String =>
    if (Option(url).nonEmpty) {
      if (url.startsWith("www.")) Some(url.split('.').drop(1).mkString("."))
      else Some(url)
    } else {
      None
    }
  }

  def testAndSend(): Unit = {
    val userPassword = spark.conf.get("spark.mlproject.password") // путь к сохраненной в HDFS spark-pipеline
    val pipelinePath = spark.conf.get("spark.mlproject.pipelinepath") // путь к сохраненной в HDFS spark-pipеline
    val sourceDataPath = spark.conf.get("spark.mlproject.sourcepath") // путь к тестовым данным
    val targetIndex = spark.conf.get("spark.mlproject.targetindex") // индекс ES, куда выдавать предсказания

    val esOptions =
      Map(
        "es.nodes" -> "10.0.1.9:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true",
        "es.net.http.auth.user" -> "artem.salnikov",
        "es.net.http.auth.pass" -> userPassword
      )

    val webLogsTestFeatures = spark.read.json(sourceDataPath)
      .withColumn("visit", explode(col("visits")))
      .withColumn("url", col("visit").getItem("url"))
      .withColumn("domain", getDomain2FromUrl(expr("parse_url(url, 'HOST')")))
      .groupBy("uid", "date")
      .agg(collect_list("domain").alias("domains"))

    /*
    val dates = webLogsTestFeatures.select("date").distinct.collect.map(_.getLong(0)).sorted

    dates.foreach { date =>
      val test = webLogsTestFeatures.filter(col("date") === date)

      val model = PipelineModel.load(pipelinePath)
      val testResult = model.transform(test)

      testResult
        .withColumn("date", lit(date))
        .write.format("es").options(esOptions)
    }
    */

    val model = PipelineModel.load(pipelinePath)
    val testResult = model.transform(webLogsTestFeatures)

    testResult
      .withColumn("gender_age", col("category"))
      .select("uid", "gender_age", "date")
      .write.format("es").options(esOptions).save(s"$targetIndex-{date}/_doc")

  }

  def main(args: Array[String]): Unit = {
    testAndSend()
  }
}
