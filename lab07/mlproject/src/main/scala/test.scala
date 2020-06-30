import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

object test extends MainWithSpark {

  val getDomain2FromUrl = udf{ url: String =>
    if (Option(url).nonEmpty) {
      if (url.startsWith("www.")) Some(url.split('.').drop(1).mkString("."))
      else Some(url)
    } else {
      None
    }
  }

  def testAndSend(): Unit = {
    val pipelinePath = spark.conf.get("spark.mlproject.pipelinepath") // путь к сохраненной в HDFS spark-pipеline
    val sourceTopic = spark.conf.get("spark.mlproject.sourcetopic") // топик Kafka, куда придут тестовые данные
    val targetTopic = spark.conf.get("spark.mlproject.targettopic") // топик Kafka, куда выдавать предсказания

    val hdfs = FileSystem.get(new Configuration())
    val path = new Path("checkpointsStreaming")
    hdfs.delete(path, true)

    val kafkaReadParams = Map(
      "kafka.bootstrap.servers" -> "10.0.1.13:6667",
      "subscribe" -> sourceTopic,
      "startingOffsets" -> raw"""earliest""",
      "enable.auto.commit" -> "false"
    )

    val kafkaWriteParams = Map(
      "kafka.bootstrap.servers" -> "10.0.1.13:6667",
      "topic" -> targetTopic,
      "checkpointLocation" -> "checkpointsStreaming"
    )

    val jsonSchema = StructType(
      StructField("uid", StringType, true) ::
        StructField("visits", ArrayType(
          StructType(
            StructField("url", StringType, true) ::
              StructField("timestamp", LongType, true) :: Nil)
        ), true) :: Nil)

    val test = spark.readStream.format("kafka").options(kafkaReadParams).load
      .select(col("value").cast("string").alias("value"))
      .withColumn("value", from_json(col("value"), jsonSchema))
      .withColumn("uid", col("value.uid"))
      .withColumn("visit", explode(col("value.visits")))
      .withColumn("url", col("visit").getItem("url"))
      .withColumn("domain", getDomain2FromUrl(expr("parse_url(url, 'HOST')")))
      .groupBy("uid")
      .agg(collect_list("domain").alias("domains"))
      .select("uid", "domains")

    val model = PipelineModel.load(pipelinePath)
    val testResult = model.transform(test)

    testResult
      .withColumn("gender_age", col("prediction"))
      .select(to_json(struct("uid", "gender_age")).alias("value"))
      .writeStream
      .format("kafka")
      .outputMode("update")
      .options(kafkaWriteParams)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    testAndSend()
  }
}
