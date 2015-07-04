import sbt._
import Keys._

organization := "almanac"
name := "almanac"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.10.5"

isSnapshot := true

libraryDependencies ++= {
  val configVersion    = "1.2.1"
  val akkaVersion      = "2.3.9"
  val logbackVersion   = "1.0.13"
  val sprayVersion     = "1.3.2"
  val sparkVersion     = "1.4.0"

  Seq(
    "com.typesafe"       %  "config"                    % configVersion,
    "com.typesafe.akka"  %% "akka-actor"                % akkaVersion exclude ("org.scala-lang" , "scala-library"),
    "com.typesafe.akka"  %% "akka-slf4j"                % akkaVersion exclude ("org.slf4j", "slf4j-api") exclude ("org.scala-lang" , "scala-library"),
    "ch.qos.logback"     %  "logback-classic"           % logbackVersion,
    "joda-time"          %  "joda-time"                 % "2.8.1",
    "org.apache.spark"   %% "spark-core"                % sparkVersion,
    "org.apache.spark"   %% "spark-streaming"           % sparkVersion,
    "org.apache.spark"   %% "spark-streaming-kafka"     % sparkVersion,
    "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M1",
    "com.twitter"        %% "chill"                     % "0.6.0",

    "io.spray"           %% "spray-testkit"             % sprayVersion % Test,
    "com.typesafe.akka"  %% "akka-testkit"              % akkaVersion  % Test,
    "org.specs2"         %% "specs2-core"               % "2.4.15"     % Test,
    "org.scalatest"      %% "scalatest"                 % "2.2.4"      % Test,
    "org.scalamock"      %% "scalamock-specs2-support"  % "3.2.1"      % Test exclude("org.specs2", "specs2")

  )
}

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-language:_",
  "-target:jvm-1.7",
  "-encoding", "UTF-8"
)

fork in run := true

crossPaths := false

parallelExecution in Test := false

assemblyJarName in assembly := "almanac.jar"