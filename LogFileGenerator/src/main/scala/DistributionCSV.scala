import HelperUtils.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
//This is the code for functionality 3
object DistributionCSV:
  val logger = CreateLogger(classOf[DistributionCSV.type])
  //Loading the configuration files so that we can fetch the regex that have been set in the .conf file
  logger.info(s"Starting DistributionCSV - loading configs")
  val config: Config = ConfigFactory.load("application.conf")
  val funcconfig = config.getConfig("randomLogGenerator.functionalityconfigs.mapreducetocsv")

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    //creating a variable one to use with the mapper.
    private final val one = new IntWritable(1)
    //creating the val word to hold our key
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //First we take the regex patterns that we want to match from the application.conf file where they have been defined.
      //Then we use matcher() to match the input value against the regex pattern.
      val injectedpattern = Pattern.compile(config.getString("randomLogGenerator.Pattern")).matcher(value.toString)
      val userdefinedpattern = Pattern.compile(funcconfig.getString("FindOccurrenceOf")).matcher(value.toString)
      //val timeinterval = value.toString.substring(0,5)
      //val matchinjected = injectedpattern.matcher(value.toString)
      //val matcheduserdefined = userdefinedpattern.matcher(value.toString)
      //If we find a string that satisfies the injected regex and is of the format we need (INFO/WARN/DEBUG/ERROR), we proceed to add it to our map output.
      if(injectedpattern.find() && userdefinedpattern.find())
      {
        logger.info(s"Found a string satisfying our regex constrains")
        //val loglevel = userdefinedpattern.group()
        //value.toString.substring(0,5) gives us the timestamp to the minute of the log message, and group() method gives us the input subsequence matched by the above matcher().
        word.set(value.toString.substring(0,5) + ":00 " + userdefinedpattern.group())
        output.collect(word, one)
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //This reducer simply adds up all the values from the map.
      val sum = values.asScala.foldLeft(0)(_+_.get)
      logger.info(s"The sum for time interval and log message type= "+ sum)
      output.collect(key, new IntWritable(sum))

  @main def runMapRed(inputPath: String, outputPath: String) =
    logger.info(s"Starting the main implementation runMapRed for DistributionCSV")
    require(!inputPath.isEmpty && !outputPath.isEmpty)
    println(inputPath)
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("DistributionCSV")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    conf.set("mapred.textoutputformat.separatorText", ",")
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    //Creating a new time format to append to our output directory
    var timeformat = new SimpleDateFormat("dd-MM-yyyy-hh-mm")
    //Saves the trouble of having to delete the output directory again and again
    FileOutputFormat.setOutputPath(conf, new Path(outputPath + funcconfig.getString("OutputPath") + "_" + timeformat.format(Calendar.getInstance().getTime)))
    logger.info(s"Job configurations set. Starting job." + conf.getJobName)
    JobClient.runJob(conf)
