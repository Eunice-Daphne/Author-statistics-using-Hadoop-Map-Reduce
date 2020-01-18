name := "eunicedaphne_johnkanagaraj_hw2"

version := "0.1"

scalaVersion := "2.13.0"


libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.8.1" ,
  "org.scalatest" %% "scalatest" % "3.0.8" % "test" ,
  "org.scalatestplus.play" %% "scalatestplus-play" % "4.0.3" % Test,
  "org.mockito" % "mockito-core" % "2.8.47" % Test ,
  "org.lucee" % "jdom" % "1.1.3",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "com.typesafe" % "config" % "1.3.2" ,
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assemblyJarName in assembly := "EuniceDaphneHW2.jar"

mainClass in(Compile, run) := Some("MapReduceDriver")

mainClass in assembly := Some("MapReduceDriver")