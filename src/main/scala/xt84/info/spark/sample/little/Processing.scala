package xt84.info.spark.sample.little

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object Processing {

  def load(spark: SparkSession, path: String): DataFrame = spark.read.format(DEFAULT_INPUT_FORMAT).options(CSV_DEFAULT_LOAD_OPTIONS).load(path)

  def prepareResult(dfDep: DataFrame, dfEmp: DataFrame): DataFrame = dfEmp.join(
    dfDep,
    dfEmp.col("dept_id") === dfDep.col("id")
  ).select(
    dfEmp.col("id").as("emp_id"),
    dfEmp.col("name").as("emp_name"),
    dfDep.col("name").as("dept_name"),
    dfEmp.col("emp_file_name")
  )

  def prepareEmployees(df: DataFrame, path: String): DataFrame = df.withColumn("emp_file_name", lit(path))

}
