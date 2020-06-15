import org.apache.spark.sql.SparkSession

abstract class MainWithSpark {
  val spark: SparkSession = SparkSession.builder()
    .getOrCreate()
}
