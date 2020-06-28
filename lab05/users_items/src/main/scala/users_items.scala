import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem => HFileSystem, Path => HPath}
import org.apache.spark.sql.functions._

object users_items extends MainWithSpark {

  def getLastHdfsDatePath(path: String): Option[(String, String)] = {
    val hfs = HFileSystem.get(new Configuration())
    val hpath = new HPath(path)
    if (hfs.exists(hpath)) {
      val lastPath = hfs.listStatus(hpath)
        .filter(_.isDirectory).map(_.getPath)
        .filter(_.getName.length == 8)
        .sortWith(_.getName > _.getName)
        .headOption
      if (lastPath.nonEmpty) Some((lastPath.get.getName, lastPath.get.toString))
      else None
    } else {
      None
    }
  }

  def getLastLocalDatePath(path: String): Option[(String, String)] = {
    val localPath = new File(path)
    if (localPath.exists()) {
      val lastPath = localPath.listFiles()
        .filter(f => f.isDirectory && f.getName.length == 8)
        .sortWith(_.getName > _.getName)
        .headOption
      if (lastPath.nonEmpty) Some((lastPath.get.getName, lastPath.get.getPath))
      else None
    } else {
      None
    }
  }

  def getLastDatePath(path: String): Option[(String, String)] = {
    if (path.startsWith("file:")) getLastLocalDatePath(path)
    else getLastHdfsDatePath(path)
  }

  def aggVisits(): Unit = {
    val isUpdate = spark.conf.getOption("spark.users_items.update").getOrElse("1").toInt
    val outputDir = spark.conf.get("spark.users_items.output_dir")
    val inputDir = spark.conf.get("spark.users_items.input_dir")

    val aggKeyColumnNames = Seq("date_part", "uid")

    val inputOriginalData = spark.read.json(inputDir)

    val inputPivotedData = inputOriginalData
      .withColumn("item_id", regexp_replace(lower(col("item_id")), "[ -]", "_"))
      .withColumn("event_item_id", concat(col("event_type"), lit("_"), col("item_id")))
      .groupBy(aggKeyColumnNames.map(col): _*)
      .pivot("event_item_id")
      .count()

    val inputMatrix = inputPivotedData.columns.filterNot(aggKeyColumnNames.contains)
      .foldLeft(inputPivotedData)((df, colName) => df.withColumn(colName, coalesce(col(colName), lit(0))))

    val lastDatePath = getLastDatePath(outputDir)

    val outputMatrixOpt =
      if (lastDatePath.nonEmpty) {
        Some(
          spark.read.parquet(lastDatePath.get._2)
            .withColumn("date_part", lit(lastDatePath.get._1))
        )
      } else {
        None
      }

    val outputMatrix =
      if (isUpdate == 1 && outputMatrixOpt.nonEmpty) outputMatrixOpt.get
      else inputMatrix.filter(lit(1) === lit(0))

    val allColumns = (inputMatrix.columns ++ outputMatrix.columns).distinct

    val inputRichMatrix = allColumns.diff(inputMatrix.columns)
      .foldLeft(inputMatrix)((df, colName) => df.withColumn(colName, lit(0)))

    val outputRichMatrix = allColumns.diff(outputMatrix.columns)
      .foldLeft(outputMatrix)((df, colName) => df.withColumn(colName, lit(0)))

    val aggregatedCols = allColumns.filterNot(aggKeyColumnNames.contains)
      .map(colName => sum(colName).alias(colName))

    val resultUnionedMatrix = inputRichMatrix.select(allColumns.map(col): _*)
      .union(outputRichMatrix.select(allColumns.map(col): _*))

    val maxDateVal = resultUnionedMatrix.agg(max("date_part").cast("string")).collect().head.getString(0)

    val resultMatrix = inputRichMatrix.select(allColumns.map(col): _*)
      .union(outputRichMatrix.select(allColumns.map(col): _*))
      .groupBy("uid")
      .agg(aggregatedCols.head, aggregatedCols.tail: _*)

    resultMatrix.write.mode("overwrite").parquet(outputDir + "/" + maxDateVal)
  }

  def main(args: Array[String]): Unit = {
    aggVisits()
  }
}
