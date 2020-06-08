import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

object data_mart extends MainWithSpark {

  val getDomain2FromUrl = udf{ url: String =>
    if (Option(url).nonEmpty) {
      if (url.startsWith("www.")) Some(url.split('.').drop(1).mkString("."))
      else Some(url)
      /*
      val domainArray = url.split('.')
      if (domainArray.length > 1) Some(domainArray.takeRight(2).mkString("."))
      else domainArray.headOption
      */
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    val userInfo: DataFrame = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()
      .withColumn("age_cat",
        when(col("age").between(18, 24), "18-24")
          .when(col("age").between(25, 34), "25-34")
          .when(col("age").between(35, 44), "35-44")
          .when(col("age").between(45, 54), "45-54")
          .when(col("age") >=55, ">=55"))
      .select("uid", "gender", "age_cat")

    val visits: DataFrame = spark.read.format("es")
      .options(Map("es.nodes" -> "10.0.1.9:9200", "es.batch.write.refresh" -> "false", "es.nodes.wan.only" -> "true"))
      .load("visits")
      .select(
        "uid", // – уникальный идентификатор пользователя, тот же, что и в базе с информацией о клиенте (в Cassandra), либо null, если в базе пользователей нет информации об этих посетителях магазина, string
        "event_type", // – buy или view, соответственно покупка или просмотр товара, string
        "category", // – категория товаров в магазине, string
        "item_id", // – идентификатор товара, состоящий из категории и номера товара в категории, string
        "item_price", // – цена товара, integer
        "timestamp" // – unix epoch timestamp в миллисекундах
      )

    val logs: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json")
      .withColumn("timestamp_url", explode(col("visits")))
      .withColumn("url", col("timestamp_url.url"))
      .withColumn("domain", getDomain2FromUrl(expr("parse_url(url, 'HOST')")))
      .select(
        "uid", // – уникальный идентификатор пользователя, тот же, что и в базе с информацией о клиенте (в Cassandra),
        "domain"
        //"visits" //массив visits c некоторым числом пар (timestamp, url), где timestamp – unix epoch timestamp в миллисекундах, url - строка.
      )

    val cats: DataFrame = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.1.9:5432/labdata?user=artem_salnikov&password=ggpY5hcd")
      .option("dbtable", "domain_cats")
      .option("driver", "org.postgresql.Driver")
      .load()
      .select("domain", "category")

    val webLogs = logs.join(cats, Seq("domain"), "inner").select("uid", "category")

    val uidShopLogs = userInfo
      .join(visits, Seq("uid"), "left")
      .withColumn("category", concat(lit("shop_"), regexp_replace(lower(col("category")), " |-", "_")))
      .select("uid", "gender", "age_cat", "category")

    val uidWebLogs = userInfo
      .join(webLogs, Seq("uid"), "left")
      .withColumn("category", concat(lit("web_"), regexp_replace(lower(col("category")), " |-", "_")))
      .select("uid", "gender", "age_cat", "category")

    val uidAllLogs = uidShopLogs.union(uidWebLogs)

    val selectedCats = uidAllLogs.select("category")
      .filter(col("category").isNotNull).distinct
      .collect().map(_.getString(0)).sorted
    val columnsExpressions =
      for (cat <- selectedCats)
        yield sum(when(col("category") === lit(cat), 1).otherwise(expr("null"))).alias(cat)

    val result = uidAllLogs
      .groupBy("uid", "gender", "age_cat")
      .agg(columnsExpressions.head, columnsExpressions.tail: _*)

    result
      .write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.1.9:5432/artem_salnikov?user=artem_salnikov&password=ggpY5hcd")
      .option("dbtable", "clients")
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite")
      .save()
  }
}
