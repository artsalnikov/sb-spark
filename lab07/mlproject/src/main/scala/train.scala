import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._

object train extends MainWithSpark {

  val getDomain2FromUrl = udf{ url: String =>
    if (Option(url).nonEmpty) {
      if (url.startsWith("www.")) Some(url.split('.').drop(1).mkString("."))
      else Some(url)
    } else {
      None
    }
  }

  def trainAndSave(): Unit = {
    val sourceDataPath = spark.conf.get("spark.mlproject.sourcepath")
    val targetModelPath = spark.conf.get("spark.mlproject.targetpath")

    val webLogsSourceFeatures = spark.read.json(sourceDataPath)
      .filter(col("uid").isNotNull)
      .na.fill(0)
      .withColumn("visit", explode(col("visits")))
      .withColumn("url", col("visit").getItem("url"))
      .withColumn("domain", getDomain2FromUrl(expr("parse_url(url, 'HOST')")))
      .groupBy("gender_age", "uid")
      .agg(collect_list("domain").alias("domains"))


    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr))

    val model = pipeline.fit(webLogsSourceFeatures)

    model.write.overwrite().save(targetModelPath)
  }

  def main(args: Array[String]): Unit = {
    trainAndSave()
  }
}
