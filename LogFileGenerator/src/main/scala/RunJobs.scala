import HelperUtils.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}

object RunJobs {
  val logger = CreateLogger(classOf[RunJobs.type])
  def main(args: Array[String]): Unit = {
    logger.info(s"Starting the main class for running all jobs together")
    val config: Config = ConfigFactory.load("application.conf")
    //val InputString=config.getString("randomLogGenerator.InputPath")
    //val OutputString=config.getString("randomLogGenerator.OutputPath")
    logger.info(s"Running Functionality 1")
    DistributionCSV.runMapRed(args(0), args(1))//Functionality 1
    logger.info(s"Running Functionality 2")
    DescendingOrderofError.runDescOrder(args(0), args(1))//Functionality 2
    logger.info(s"Running Functionality 3")
    NumberofMessages.runNoOfMsg(args(0), args(1))//Functionality 3
    logger.info(s"Running Functionality 4")
    LongestString.runLongestString(args(0), args(1))//Functionality 4
    logger.info(s"Jobs complete")
  }
}
