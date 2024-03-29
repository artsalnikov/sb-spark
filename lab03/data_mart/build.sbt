name := "data_mart"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.5"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.5"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.7.0"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.4.3"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.12"
