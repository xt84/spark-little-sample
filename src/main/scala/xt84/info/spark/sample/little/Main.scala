package xt84.info.spark.sample.little

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.util.{Failure, Success}

object Main extends App with LazyLogging {

  override def main(args: Array[String]): Unit = {
    if ((args.length == 0) || ((args.length % KEYS.size) > 0) || (args.length < 3)) {
      println(usage(KEYS))
      System.exit(1)
    } else {
      val parameters = parseArguments(args.toList)
      val job = new Job(initSession(), parameters)
      job.run() match {
        case Success(_)   => logger.info("All OK")
        case Failure(e)   => logger.error("Exception while processing data: ", e)
      }
    }
  }

}
