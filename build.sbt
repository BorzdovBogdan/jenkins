name := "spark_elastic"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0" % "provided"
libraryDependencies +="org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies +="org.apache.spark" %% "spark-sql" % "2.3.0"
//libraryDependencies +="org.apache.spark" %% "spark-avro" % "2.3.0"
// https://mvnrepository.com/artifact/com.databricks/spark-avro
libraryDependencies += "/org.apache" %% "spark-avro" % "2.4.0"


