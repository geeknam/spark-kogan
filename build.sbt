
name := "spark-kogan"
homepage := Some(url("https://kogan.com/api/v1"))
description := "Kogan.com data Spark Datasource."

organization := "com.kogan"
version := sys.env.get("ARTEFACT_VERSION").getOrElse("0.1-SNAPSHOT")

scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"


//Publish assembly fat jar
artifact in(Compile, assembly) := {
  val art = (artifact in(Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}
addArtifact(artifact in(Compile, assembly), assembly)

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

// HTTP client lib
libraryDependencies += "com.lihaoyi" %% "requests" % "0.1.9"

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"


// Creating Scala Fat Jars for Spark on SBT
// Source: https://stackoverflow.com/questions/23280494/sbt-assembly-error-deduplicate-different-file-contents-found-in-the-following
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// test run settings
parallelExecution in Test := false
fork in Test := true
fork in IntegrationTest := true
assembly / test := {}

// Enable integration tests
Defaults.itSettings
lazy val FunTest = config("it") extend (Test)
lazy val root = project.in(file(".")).configs(FunTest)

// Measure time for each test
Test / testOptions += Tests.Argument("-oD")
IntegrationTest / testOptions += Tests.Argument("-oD")
