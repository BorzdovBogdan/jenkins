name := "jenkins_demo_app"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "3.0.0",
"org.apache.spark" %% "spark-sql" % "3.0.0",
//libraryDependencies +="org.apache.spark" %% "spark-avro" % "2.3.0",
// https://mvnrepository.com/artifact/com.databricks/spark-avro,
"org.apache.spark" %% "spark-avro" % "3.0.0",
//"org.elasticsearch" %% "elasticsearch-spark-20" % "7.8.0",
"org.scalatest" %% "scalatest" % "3.2.2" % Test,
"org.mockito" %% "mockito-scala" % "1.16.23" % Test)


