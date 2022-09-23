import HelperUtils.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}

object RunJobs {
  val logger = CreateLogger(classOf[RunJobs.type])
  def main(args: Array[String]): Unit = {
    logger.info(s"Starting the main class for running all jobs together")
    val config: Config = ConfigFactory.load("application.conf")
    val InputString=config.getString("randomLogGenerator.InputPath")
    val OutputString=config.getString("randomLogGenerator.OutputPath")
    logger.info(s"Running Functionality 1")
    DistributionCSV.runMapRed(InputString, OutputString)//Functionality 1
    logger.info(s"Running Functionality 2")
    DescendingOrderofError.runDescOrder(InputString, OutputString)//Functionality 2
    logger.info(s"Running Functionality 3")
    NumberofMessages.runNoOfMsg(InputString, OutputString)//Functionality 3
    logger.info(s"Running Functionality 4")
    LongestString.runLongestString(InputString, OutputString)//Functionality 4
    logger.info(s"Jobs complete")
  }
}
