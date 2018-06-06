name := "pipeline-examples"

lazy val examples = (project in file("."))
  .aggregate(basic)

lazy val pipeline = (project in file(".."))

lazy val basic = (project in file("./basic"))
  .settings(commonSettings)
  .dependsOn(pipeline)

def commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.commons" % "commons-lang3" % "3.7",
    "commons-io" % "commons-io" % "2.6",
    "com.typesafe.akka" %% "akka-stream" % "2.5.8"
  )
)
