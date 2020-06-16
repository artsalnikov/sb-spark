import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object filter extends MainWithSpark {

  def importVisits(): Unit = {
    val topicName = spark.conf.get("spark.filter.topic_name")
    val startingOffsets = spark.conf.get("spark.filter.offset") match {
      case "earliest" => raw"""earliest"""
      case "latest" => raw"""latest"""
      case s: String => raw"""{"$topicName":{"0":$s}}"""
    }
    val pathToSave = spark.conf.get("spark.filter.output_dir_prefix")
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "10.0.1.13:6667",
      "subscribe" -> topicName,
      "startingOffsets" -> startingOffsets,
      "enable.auto.commit" -> "false"
    )

    val messages = spark.read.format("kafka").options(kafkaParams).load
      .select(col("value").cast("string").alias("value"))

    val jsonSchema = StructType(
      StructField("event_type", StringType, true) ::
        StructField("category", StringType, true) ::
        StructField("item_id", StringType, true) ::
        StructField("item_price", LongType, true) ::
        StructField("uid", StringType, true) ::
        StructField("timestamp", LongType, true) :: Nil)

    val messagesDf = messages
      .withColumn("value", from_json(col("value"), jsonSchema))
      .select("value.*")
      .withColumn("date",
        regexp_replace(from_unixtime(col("timestamp") / 1000).cast("date").cast("string"), "-", ""))
      .withColumn("event_type_part", col("event_type"))
      .withColumn("date_part", col("date"))

    messagesDf.write.mode("overwrite").partitionBy("event_type_part", "date_part").json(pathToSave)
  }

  def main(args: Array[String]): Unit = {
    importVisits()
  }
}
