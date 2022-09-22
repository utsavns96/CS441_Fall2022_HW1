import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funsuite.AnyFunSuite

import java.util.regex.Pattern

object TestCases
  class TestCase extends AnyFunSuite:
    test("Unit test for config load") {
      //test if the config file loads or not
      val config: Config = ConfigFactory.load("application.conf")
      val funcconfig = config.getConfig("randomLogGenerator.functionalityconfigs.longeststring")
      assert(!config.isEmpty && !funcconfig.isEmpty)
    }

    test("Unit test for OutputPath"){
      //test if the config for OutputPath is present in the application.conf file
      val config: Config = ConfigFactory.load("application.conf")
      val funcconfig = config.getConfig("randomLogGenerator.functionalityconfigs.longeststring")
      val outputpath = funcconfig.getString("OutputPath")
      assert(!outputpath.isEmpty)
    }

    test("Unit test for user defined regex positive") {
      //check if we are matching our user defined pattern correctly
      val config: Config = ConfigFactory.load("application.conf")
      val funcconfig = config.getConfig("randomLogGenerator.functionalityconfigs.longeststring")
      val value="14:35:50.652 [scala-execution-context-global-21] ERROR HelperUtils.Parameters$ - P#~\"PoX@Oc+f!&Q4h3TM:ioE(+B(\"\"`*3U2y;2~[hQL1Js{Iez<(A&CP"
      val userdefinedpattern = Pattern.compile(funcconfig.getString("FindOccurrenceOf")).matcher(value.toString)
      assert(userdefinedpattern.find())
    }

    test("Unit test for user defined regex negative") {
      //check if we are matching our user defined pattern correctly
      val config: Config = ConfigFactory.load("application.conf")
      val funcconfig = config.getConfig("randomLogGenerator.functionalityconfigs.longeststring")
      val value = "@org.apache.hadoop.metrics2.annotation.Metric(sampleName=Ops, always=false, valueName=Time, about=, interval=10, type=DEFAULT, value=[])"
      val userdefinedpattern = Pattern.compile(funcconfig.getString("FindOccurrenceOf")).matcher(value.toString)
      assert(!userdefinedpattern.find())
    }

    test("Unit test for injected regex negative") {
      //check if we are matching our injected pattern correctly. It should not match in this case.
      val config: Config = ConfigFactory.load("application.conf")
      val value = "14:35:50.652 [scala-execution-context-global-21] ERROR HelperUtils.Parameters$ - P#~\"PoX@Oc+f!&Q4h3TM:ioE(+B(\"\"`*3U2y;2~[hQL1Js{Iez<(A&CP"
      val injectedpattern = Pattern.compile(config.getString("randomLogGenerator.Pattern")).matcher(value.toString)
      assert(!injectedpattern.find())
    }

    test("Unit test for injected regex positive") {
      //check if we are matching our injected pattern correctly. It should match in this case.
      val config: Config = ConfigFactory.load("application.conf")
      val value = "14:35:49.958 [scala-execution-context-global-21] INFO  HelperUtils.Parameters$ - hxgQ_i:JDGT7hN7wbg3ae0cg0ag2NG-xk\\Bcb."
      val injectedpattern = Pattern.compile(config.getString("randomLogGenerator.Pattern")).matcher(value.toString)
      assert(injectedpattern.find())
    }
