package xt84.info.spark.sample

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

package object litle {

  val DEFAULT_INPUT_FORMAT  = "com.databricks.spark.csv"
  val DEFAULT_OUTPUT_FORMAT = "parquet"

  val KEYS = Map(
    "dep"     -> "Path to departments data",
    "emp"     -> "Path to employees data",
    "output"  -> "Path to result"
  )

  val CSV_DEFAULT_LOAD_OPTIONS = Map(
    "header"      -> "true",
    "inferSchema" -> "true",
    "delimiter"   -> ";"
  )

  def usage(keys: Map[String, String]): String = keys.map(
    kv => s"--${kv._1} ${kv._2}"
  ).toList.mkString("\n")

  def parseArguments(args: List[String], argsMap: Map[String, String] = Map()): Map[String, String] = {
    def isKey(arg: String) = arg.startsWith("--")
    args match {
      case Nil => argsMap
      case arg :: args if isKey(arg) => parseArguments(
        args.tail,
        argsMap ++ Map(arg.replaceAll("--", "") -> args.head)
      )
    }
  }

  def initSession(appName: String = "SparkApp", master: Option[String] = None, options: Option[Map[String, String]] = None): SparkSession =
    SparkSession.getActiveSession.getOrElse({
      val sessionBuilder = SparkSession.builder().appName(appName)
      if (master.isDefined) sessionBuilder.master(master.get)
      if (options.isDefined) sessionBuilder.config(new SparkConf().setAll(options.get)).getOrCreate() else sessionBuilder.getOrCreate()
    })
}
