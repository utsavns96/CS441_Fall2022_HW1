import HelperUtils.CreateLogger
import java.text.SimpleDateFormat
import java.util.Calendar
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
  //This is the implementation for Functionality 4
  object LongestString:
    val logger = CreateLogger(classOf[DistributionCSV.type])
    logger.info(s"Starting LongestString - loading configs")
    //Loading the configuration files so that we can fetch the regex that have been set in the .conf file
    val config: Config = ConfigFactory.load("application.conf")
    val funcconfig = config.getConfig("randomLogGenerator.functionalityconfigs.longeststring")
    class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
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
          //Setting the key as the the log message type
          word.set(userdefinedpattern.group())
          //Adding to our output the key and the length of the pattern that we found from the matcher() method
          output.collect(word, new IntWritable(injectedpattern.group().length))
        }

    class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
      override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
        //This reducer finds the maximum value from the output of our map.
        val longest = values.asScala.max
        logger.info(s"The longest string for= "+ key +" is of length "+ longest)
        output.collect(key, longest)

    @main def runLongestString(inputPath: String, outputPath: String) =
      logger.info(s"Starting the main implementation runLongestString for LongestString")
      require(!inputPath.isEmpty && !outputPath.isEmpty)
      println(inputPath)
      //setting job parameters
      val conf: JobConf = new JobConf(this.getClass)
      conf.setJobName("LongestString")
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
      FileOutputFormat.setOutputPath(conf, new Path(outputPath + funcconfig.getString("OutputPath")+"_"+timeformat.format(Calendar.getInstance().getTime)))
      logger.info(s"Job configurations set. Starting job." + conf.getJobName)
      JobClient.runJob(conf)

