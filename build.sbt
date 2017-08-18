enablePlugins(ScalafmtPlugin)

lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.example",
      scalaVersion := "2.11.11",
      version := "0.1.0-SNAPSHOT"
    )),
  name := "embulk-filter_key_in_redis",
  scalafmtOnCompile in ThisBuild := true,
  scalafmtTestOnCompile in ThisBuild := true
)

resolvers += Resolver.jcenterRepo
resolvers += Resolver.sonatypeRepo("releases")
resolvers += "velvia maven" at "http://dl.bintray.com/velvia/maven"

lazy val circeVersion = "0.8.0"
libraryDependencies ++= Seq(
  "org.jruby" % "jruby-complete" % "1.6.5",
  "org.embulk" % "embulk-core" % "0.8.25",
  "com.github.etaty" %% "rediscala" % "1.7.0",
  "org.bouncycastle" % "bcpkix-jdk15on" % "1.57",
  "com.github.pathikrit" %% "better-files" % "2.17.1",
  "org.scalaz" %% "scalaz-core" % "7.2.14",
  "org.scalaz" %% "scalaz-concurrent" % "7.2.14",
  "org.velvia" %% "msgpack4s" % "0.6.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)
