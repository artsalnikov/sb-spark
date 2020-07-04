name := "dashboard"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.5"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.5"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.4.5"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.7.0"

