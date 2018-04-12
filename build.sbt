name := "BatchGeocodingInScalaUsingGoogleAPI"

version := "1.0"

scalaVersion := "2.12.2"

lazy val akkaVersion = "2.5.11"
lazy val akkaHttpVersion = "10.0.11"
lazy val playVersion     = "2.6.2"

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.typesafe.play" %% "play-json" % playVersion,
  "com.typesafe.play" %% "anorm" % "2.5.3",
  "com.typesafe.slick" %% "slick" % "3.2.3",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.2.3",
  "mysql" % "mysql-connector-java" % "5.1.40",
  "org.postgresql" % "postgresql" % "42.1.3",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "org.apache.sis.core" % "sis-referencing" % "0.7",
  "com.github.nikita-volkov" % "sext" % "0.2.4",
  "com.univocity" % "univocity-parsers" % "2.5.8",
  "com.ibm.icu" % "icu4j" % "59.1",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "0.18",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.slf4j" % "slf4j-nop" % "1.6.4"
)
