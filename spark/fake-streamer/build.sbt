ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "fake-streamer"
  )

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.12.183",
)

val SparkVersion = "3.2.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-streaming" % SparkVersion,
)
