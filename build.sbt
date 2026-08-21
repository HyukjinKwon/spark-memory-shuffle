// spark-memory-shuffle: an in-memory ShuffleManager plugin for Apache Spark.
//
// The plugin compiles against a *released* Apache Spark (spark-core is `provided`),
// so it links against the plain `org.apache.spark.shuffle.ShuffleManager` trait.

ThisBuild / organization := "io.github.hyukjinkwon"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

// Apache Spark to build against. 4.2.0 is the target release.
val sparkVersion = "4.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-memory-shuffle",

    // spark-core is `provided`: it is supplied by the Spark runtime the plugin is
    // dropped into, and must never be bundled into the plugin jar.
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),

    // Match Spark's own compiler hygiene reasonably closely.
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-encoding",
      "UTF-8"
    ),

    // Spark tests spin up a local SparkContext; on JDK 17+ they need these opens.
    Test / fork := true,

    // Let the local-cluster launcher resolve the Scala version and take the "testing" classpath
    // path (it then trusts spark.executor.extraClassPath instead of a distribution jars/ dir).
    Test / envVars ++= Map(
      "SPARK_TESTING" -> "1",
      "SPARK_SCALA_VERSION" -> "2.13"
    ),
    // Forward any -Dbench.* properties given to sbt into the forked test JVM, so the benchmark
    // (MemoryShuffleBenchmark) can be tuned from the command line despite Test / fork := true.
    Test / javaOptions ++= sys.props.collect {
      case (k, v) if k.startsWith("bench.") => s"-D$k=$v"
    }.toSeq,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    )
  )
