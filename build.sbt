import Common._
import sbt.Keys._

organization := "io.ddf"

name := "ddf"

version := ddfVersion

retrieveManaged := true // Do create a lib_managed, so we have one place for all the dependency jars to copy to slaves, if needed

scalaVersion := theScalaVersion

scalacOptions := Seq("-unchecked", "-optimize", "-deprecation")

// Fork new JVMs for tests and set Java options for those
fork in Test := true

parallelExecution in ThisBuild := false

javaOptions in Test ++= Seq("-Xmx2g")

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

conflictManager := ConflictManager.strict

commonSettings

lazy val root = project.in(file(".")).aggregate(jdbc, jdbcExamples,jdbcTest)

val com_adatao_unmanaged = Seq(
  "com.adatao.unmanaged.net.rforge" % "REngine" % "2.1.1.compiled",
  "com.adatao.unmanaged.net.rforge" % "Rserve" % "1.8.2.compiled"
)

lazy val jdbc = project.in(file("jdbc")).settings(commonSettings: _*).settings(
  name := "ddf-jdbc",
  pomExtra := submodulePom,
  libraryDependencies ++= Seq(
    "io.ddf" %% "ddf_core" % ddfVersion,
    "com.zaxxer" % "HikariCP-java6" % "2.3.9",
    "org.scalikejdbc" %% "scalikejdbc" % "2.2.7",
    "com.univocity" % "univocity-parsers" % "1.5.5",
    "com.clearspring.analytics" % "stream" % "2.4.0" exclude("asm", "asm")
  )
)

lazy val jdbcExamples = project.in(file("jdbc-examples")).dependsOn(jdbc).settings(commonSettings: _*).settings(
  name := "jdbc-examples",
  pomExtra := submodulePom
)

lazy val jdbcTest = project.in(file("jdbc-test")).dependsOn(jdbc).settings(commonSettings: _*).settings(
  name := "jdbc-test",
  pomExtra := submodulePom,
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.0-M7",
    "com.h2database" % "h2" % "1.4.187" % "test"
  )
)

resolvers ++= Seq("Adatao Mvnrepos Snapshots" at "https://raw.github.com/adatao/mvnrepos/master/snapshots",
  "Adatao Mvnrepos Releases" at "https://raw.github.com/adatao/mvnrepos/master/releases")

publishMavenStyle := true
