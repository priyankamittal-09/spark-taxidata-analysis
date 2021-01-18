name := "spark-taxidata-analysis"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVer = "3.1.0"
val specs2Version = "4.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVer,
  "org.apache.spark" %% "spark-sql" % sparkVer,
  "org.typelevel" %% "cats" % "0.9.0"
)

libraryDependencies ++= Seq (
  "org.specs2"                 %% "specs2-core"                    % specs2Version  ,
  "org.specs2"                 %% "specs2-mock"                    % specs2Version  ,
  "org.specs2"                 %% "specs2-matcher-extra"           % specs2Version
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}