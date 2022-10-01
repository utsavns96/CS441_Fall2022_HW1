import HelperUtils.{CreateLogger, ExtensionRenamer}
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
object NumberofMessages :
  val logger = CreateLogger(classOf[NumberofMessages.type])
  //Loading the configuration files so that we can fetch the regex that have been set in the .conf file
  logger.info(s"Starting DistributionCSV - loading configs")
  val config: Config = ConfigFactory.load("application.conf")
  val funcconfig = config.getConfig("randomLogGenerator.functionalityconfigs.NumberofMsg")

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    //creating a variable one to use with the mapper.
    private final val one = new IntWritable(1)
    //creating the val word to hold our key
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val userdefinedpattern = Pattern.compile(funcconfig.getString("FindOccurrenceOf")).matcher(value.toString)
      if (userdefinedpattern.find())
        logger.debug(s"Found a string satisfying our regex constrains: "+userdefinedpattern.group())
        word.set(userdefinedpattern.group())
        output.collect(word, one)


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //This reducer simply adds up all the values from the map.
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      logger.debug(s"The sum for time interval and log message type= " + sum)
      output.collect(key, new IntWritable(sum))
  //@main
  def runNoOfMsg(inputPath: String, outputPath: String) =
    logger.info(s"Starting the main implementation runMapRed for DistributionCSV")
    require(!inputPath.isEmpty && !outputPath.isEmpty)
    println(inputPath)
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("NumberofMsg")
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
    conf.set("mapred.textoutputformat.separator", ",")
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    //Creating a new time format to append to our output directory
    var timeformat = new SimpleDateFormat("dd-MM-yyyy-hh-mm-ss")
    //Saves the trouble of having to delete the output directory again and again
    //specifically using a variable here to then pass it onto changeExt to rename the file.
    val outpath = outputPath + funcconfig.getString("OutputPath") + "_" + timeformat.format(Calendar.getInstance().getTime)
    FileOutputFormat.setOutputPath(conf, new Path(outpath))
    logger.info(s"Job configurations set. Starting job." + conf.getJobName)
    JobClient.runJob(conf)
    ExtensionRenamer.changeExt(outpath,conf.getJobName)
