scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.4" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % "3.3.4" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.hadoop" % "hadoop-client" % "2.8.2"
)

dependencyOverrides ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"
)

resolvers ++= Seq(
  "Apache Spark Maven Repo" at "https://repo1.maven.org/maven2/org/apache/spark"
)