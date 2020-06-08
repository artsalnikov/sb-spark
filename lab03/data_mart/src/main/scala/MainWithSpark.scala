import org.apache.spark.sql.SparkSession

abstract class MainWithSpark {
  val spark: SparkSession = SparkSession.builder()
    .config("spark.cassandra.connection.host", "10.0.1.9")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
}
