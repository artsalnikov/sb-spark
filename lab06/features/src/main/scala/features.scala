import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object features extends MainWithSpark {

  def calcNorm(vector: SparseVector): Array[Double] = {
    if (vector != null) vector.toDense.toArray
    else Array.empty[Double]
  }

  def visitsItems(): Unit = {
    val webLogs = spark.read.json("/labs/laba03/weblogs.json")
      .filter(col("uid").isNotNull)

    val visits = spark.read.parquet("users-items/20200429")

    val webVisitsParsed = webLogs
      .select(
        col("uid"),
        explode(col("visits")).alias("visit")
      )
      .withColumn("timestamp", col("visit.timestamp") / 1000)
      .select(
        col("uid"),
        from_unixtime(col("timestamp").cast("int")).alias("timestamp"),
        col("visit.url").alias("url")
      )
      .withColumn("host", expr("parse_url(url, 'HOST')"))
      .withColumn("domain", lower(regexp_replace(col("host"), "^(www.)", "")))

    val webVisits = webVisitsParsed.withColumn("week_day_abb", lower(date_format(col("timestamp"), "E")))
      .withColumn("day_hour", date_format(col("timestamp"), "HH"))
      .withColumn("all_visits", lit(1))
      .withColumn("day_visits", expr("case when day_hour > '09' and day_hour <= '18' then 1 else 0 end" ))
      .withColumn("evening_visits", expr("case when day_hour > '18' then 1 else 0 end" ))

    val hrs = 0 to 23
    val webHourVisits = hrs.foldLeft(webVisits){(df, hour) =>
        val col_ = s"web_hour_$hour"
        val equ_ = "%02d".format(hour)
        df.withColumn(col_, when(col("day_hour") === equ_, 1).otherwise(0))
      }

    val agg_hour_ = hrs.map{ x =>
      val col_ = s"web_hour_$x"
      sum(col(col_)).alias(col_)
    }

    val wd = Seq("mon", "tue", "wed", "thu", "fri", "sat", "sun")
    val webDayHourVisits = wd.foldLeft(webHourVisits){(df, weekDay) =>
      val col_ = s"web_day_$weekDay"
      df.withColumn(col_, when(col("week_day_abb") === weekDay, 1).otherwise(0))
    }

    val agg_wd_ = wd.map{ x =>
      val col_ = s"web_day_$x"
      sum(col(col_)).alias(col_)
    }

    val agg_x_ = Seq(
      sum(col("day_visits")).alias("day_visits"),
      sum(col("evening_visits")).alias("evening_visits"),
      sum(col("all_visits")).alias("all_visits")
    ) ++ agg_wd_ ++ agg_hour_

    val top1000 = webVisits.filter(col("domain") =!= "null")
      .groupBy("domain")
      .count
      .withColumn("rn", row_number().over(Window.orderBy(col("count").desc, col("domain"))))
      .filter(col("rn") <= 1000)

    val webVisitsDomainGrouped = webDayHourVisits.join(broadcast(top1000), Seq("domain"), "left")
      .withColumn("domain", when(col("count").isNull, lit("NULL")).otherwise(col("domain")))
      .groupBy("uid")
      .agg(collect_list(col("domain")).alias("domain_list"), `agg_x_`: _*)

    val myVocabulary = top1000.select("domain").collect.map(_.getString(0)).sorted

    val cvModel: CountVectorizerModel = new CountVectorizerModel(myVocabulary).setInputCol("domain_list")
      .setOutputCol("domain_vector")

    val webVisitsVector = cvModel.transform(webVisitsDomainGrouped)
      .withColumn("web_fraction_work_hours", col("day_visits") / col("all_visits"))
      .withColumn("web_fraction_evening_hours", col("evening_visits") / col("all_visits"))
      .drop(col("day_visits"))
      .drop(col("evening_visits"))
      .drop(col("all_visits"))
      .drop(col("domain_list"))

    val calcNormDF = udf[Array[Double], SparseVector](calcNorm)

    val webVisitsResult = webVisitsVector.join(visits, Seq("uid"), "full")
      .withColumn("domain_features", calcNormDF(col("domain_vector"))).drop(col("domain_vector"))

    webVisitsResult.write.mode("overwrite").parquet("features")
  }

  def main(args: Array[String]): Unit = {
    visitsItems()
  }
}
