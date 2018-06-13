package xt84.info.spark.sample.litle

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try

class Job(val spark: SparkSession, val parameters: Map[String, String]) {

  def run() = Try(
    Processing.prepareResult(
      Processing.load(spark, parameters("dep")),
      Processing.prepareEmployees(
        Processing.load(spark, parameters("emp")),
        parameters("emp")
      )
    ).write
      .format(DEFAULT_OUTPUT_FORMAT)
      .mode(SaveMode.Overwrite)
      .save(parameters("output"))
  )

}
