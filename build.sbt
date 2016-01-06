// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

// Project name & version
name := """IoT-Spark-Basics"""
version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.7"
val sparkVersion = "1.6.0"

// Needed as SBT's classloader doesn't work well with Spark
fork := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M3"
)

// logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"


/// console

// define the statements initially evaluated when entering 'console'

val sparkMode = sys.env.getOrElse("SPARK_MODE", "local[2]")

initialCommands += """
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

  val conf = new SparkConf()
     .setAppName("Spark Console")
     .setMaster("local[*]")
     .set("spark.cassandra.connection.host", "localhost")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
                   """

cleanupCommands += """
  println("Closing the SparkContext:")
  sc.stop()
                   """
/*
initialCommands in console :=
  s"""
    |import org.apache.spark.SparkConf
    |import org.apache.spark.SparkContext
    |import org.apache.spark.SparkContext._
    |
    |@transient val sc = new SparkContext(
    |  new SparkConf()
    |    .setMaster("$sparkMode")
    |    .setAppName("Console test"))
    |implicit def sparkContext = sc
    |import sc._
    |
    |@transient val sqlc = new org.apache.spark.sql.SQLContext(sc)
    |implicit def sqlContext = sqlc
    |import sqlc._
    |
    |def time[T](f: => T): T = {
    |  import System.{currentTimeMillis => now}
    |  val start = now
    |  try { f } finally { println("Elapsed: " + (now - start)/1000.0 + " s") }
    |}
    |
    |""".stripMargin

cleanupCommands in console :=
  s"""
     |sc.stop()
   """.stripMargin
*/
