enablePlugins(AssemblyPlugin)

name := "spark-weave-shuffle"
version := "0.1.0"
scalaVersion := "2.12.17"
organization := "org.apache.spark.shuffle.weave"

import sbtprotoc.ProtocPlugin.autoImport._


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.2" excludeAll(
    ExclusionRule(organization = "io.netty")
  ),
  "org.apache.spark" %% "spark-sql" % "3.2.2" excludeAll(
    ExclusionRule(organization = "io.netty")
  ),
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)


Compile / PB.targets := Seq(
  PB.gens.java -> (Compile / sourceManaged).value,
  scalapb.gen(javaConversions = true) -> (Compile / sourceManaged).value
)

ThisBuild / versionScheme := Some("early-semver")
ThisBuild / resolvers += Resolver.sonatypeRepo("releases")

assembly / assemblyMergeStrategy := {
  case PathList("git.properties") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
