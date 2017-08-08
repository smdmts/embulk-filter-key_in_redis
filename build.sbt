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

libraryDependencies ++= Seq(
  "org.jruby" % "jruby-complete" % "1.6.5",
  "org.embulk" % "embulk-core" % "0.8.25",
  "com.typesafe.akka" %% "akka-actor" % "2.5.3",
  "com.github.etaty" %% "rediscala" % "1.7.0",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.5" % Test
)
