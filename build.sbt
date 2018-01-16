name := "pipeline"
organization := "com.commodityvectors"

scalaVersion := "2.12.4"
crossScalaVersions := Seq("2.11.8", "2.12.4")

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "joda-time" % "joda-time" % "2.9.9",
  "com.typesafe.akka" %% "akka-stream" % "2.5.9",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.9",
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "org.scalamock" %% "scalamock" % "4.0.0" % Test
)

scalafmtOnCompile in ThisBuild := true

bintrayOrganization := Some("commodityvectors")
bintrayRepository := "commodityvectors-releases"
bintrayPackageLabels := Seq("akka-stream", "snapshot", "persistence")

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))
pomExtra := (
  <scm>
    <url>git@github.com:commodityvectors/pipeline.git</url>
    <connection>scm:git:git@github.com:commodityvectors/pipeline.git</connection>
  </scm>
)

releaseCrossBuild := true
