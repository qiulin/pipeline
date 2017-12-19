name := "pipeline-examples"

lazy val examples = (project in file("."))
  .aggregate(basic)

lazy val basic = (project in file("./basic"))
  .settings(commonSettings)

def commonSettings = Seq(
  libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-stream" % "2.5.8",
    "com.commodityvectors" %% "pipeline" % "0.1-SNAPSHOT"
  )
)