name := "spark-samples"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.3"
libraryDependencies += "com.crealytics" %% "spark-excel" % "3.0.3_0.16.0"

//this is for test only
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.10" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"

ThisBuild / evictionErrorLevel := Level.Warn
