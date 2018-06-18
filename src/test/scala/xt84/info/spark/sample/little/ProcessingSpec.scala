package xt84.info.spark.sample.little

import java.io.File
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FeatureSpec, GivenWhenThen}

import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class ProcessingSpec extends FeatureSpec with GivenWhenThen with BeforeAndAfter {

  val spark: SparkSession = initSession(appName = "TEST", master = Some("local"))
  import spark.implicits._

  val inputFileEmp: String = getClass.getResource("/raw_files/emp.txt").getPath
  val inputFileDep: String = getClass.getResource("/raw_files/dep.txt").getPath

  val columns = Seq("emp_id", "emp_name", "dept_name", "emp_file_name")

  val expectedJson: String = List(
    (1, "Oleg", "IT department", inputFileEmp),
    (2, "Ivan", "IT department", inputFileEmp),
    (3, "Ashot", "Marketing department", inputFileEmp)
  ).toDF(columns:_*).orderBy("emp_id").toJSON.collect().mkString("\n")

  feature("Load data, prepare and write to Parquet") {
    info("Two data files (CSV with ';' splitter, first row is header), must be joined")
    info("Expected result file must be like this:")
    info("emp_id | emp_name | dept_name | emp_file_name")

    scenario("Load data, prepare input data about employees and result join") {
      Given("Input data and expected final dataset")
        val dfDep = Processing.load(spark, inputFileDep)
        val dfEmp = Processing.load(spark, inputFileEmp)
      When("Prepare employees dataset and prepare result")
        val dfFinal = Processing.prepareResult(dfDep, Processing.prepareEmployees(dfEmp, inputFileEmp))
      Then("Final dataset and expected dataset must be equal")
        assert(dfFinal.orderBy("emp_id").toJSON.collect().mkString("\n") == expectedJson)
    }
    scenario("Run job flow as in Job runner") {
      Given("Parameters Map and Job class object")
        val parameters = Map(
          "dep"     -> inputFileDep,
          "emp"     -> inputFileEmp,
          "output"  -> s"${System.getProperty("java.io.tmpdir")}/${UUID.randomUUID().toString}"
        )
        val job = new Job(spark, parameters)
      When("Run processing")
        val result = job.run()
      Then("No exceptions and files must be writed")
        result match {
          case Failure(e) =>
            println(e.getMessage)
            assert (false)
          case Success(_) =>
            assert(new File(s"${parameters("output")}/_SUCCESS").exists())
          case _ => assert(false)
        }
    }
  }
}
