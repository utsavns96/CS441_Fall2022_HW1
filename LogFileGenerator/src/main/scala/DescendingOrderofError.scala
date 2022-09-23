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
import org.apache.hadoop.mapred.RunningJob
import org.apache.hadoop.mapred.jobcontrol.{Job, JobControl}

import scala.language.postfixOps

object DescendingOrderofError:
  val logger = CreateLogger(classOf[DistributionCSV.type])
  logger.info(s"Starting LongestString - loading configs")
  //Loading the configuration files so that we can fetch the regex that have been set in the .conf file
  val config: Config = ConfigFactory.load("application.conf")
  val funcconfig = config.getConfig("randomLogGenerator.functionalityconfigs.descendingorder")

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    //creating the val word to hold our key
    private val word = new Text()
    private final val one = new IntWritable(1)

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
      if (injectedpattern.find() && userdefinedpattern.find()) {
        logger.info(s"Found a string satisfying our regex constrains")
        //Here we need to calculate the time intervals
        val time = value.toString.substring(0,8)
        val second = value.toString.substring(6,8)
        println("Time: "+time)
        println("Second: "+second)
        //if(second<59)
        //Setting the key as the the log message type
        word.set(value.toString.substring(0, 5) + ":00->" + userdefinedpattern.group())
        output.collect(word, one)
      }

  class SortMap extends MapReduceBase with Mapper[LongWritable, Text, IntWritable, Text] :
    //private val word = new Text()
    //private val mapcount = new IntWritable()

    def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit = {
      logger.info(s"Running the mapper for sorting the output")
      //println("Starting SortMap")
      //println("Full input= " + value.toString)
      val line: Array[String] = value.toString.split(",")
      //now we have key as line[0] and value as line[1]
      //We are going to take advantage of the fact that Map sorts the keys in ascending order before passing to the reducer.
      //Step 1 is to reverse the order of key and value, so that mapper sorts on the count of entries for the time interval
      //Step 2 is to multiply the old value by -1, so that the mapper arranges them in ascending order - larger negative numbers are smaller so they will come on top
      //We will revert this multiplication in the reducer.
      //println("SortMapKey= " + line(0) + "\nSortMapVal=" + line(1).toInt)
      //word.set(line(0))
      //println("Word has been set =" + word + " " + word.getClass.getName)
      //mapcount.set((-1 * line(1).toInt))
      //println("mapcount has been set =" + mapcount + " " + mapcount.getClass.getName)
      output.collect(new IntWritable(-1 * line(1).toInt), new Text(line(0)))
    }



  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //This reducer finds the maximum value from the output of our map.
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      logger.info(s"The sum for time interval and log message type= " + sum)
      output.collect(key, new IntWritable(sum))

  class SortReduce extends MapReduceBase with Reducer[IntWritable, Text, Text, IntWritable] :
   // private val word = new Text()
    //private val mapcount = new IntWritable()
    def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit ={
      logger.info(s"Running the reducer for sorting the output")
      //println("SortReduceKey= "+)
     // println("Starting SortReduce")
     // println("Full input= "+key)
      //mapcount.set(-1*key.toString.toInt)
      values.asScala.foreach(token =>
       // println("Token= "+token)
       // word.set(token)
       // println("Word has been set ="+word+" "+word.getClass.getName)
       // println("mapcount has been set ="+mapcount+" "+mapcount.getClass.getName)
        output.collect(token, new IntWritable(-1*key.toString.toInt))
      )
    }


  @main def runDescOrder(inputPath: String, outputPath: String) =
    logger.info(s"Starting the main implementation runLongestString for LongestString")
    require(!inputPath.isEmpty && !outputPath.isEmpty)
    println(inputPath)
    //setting job parameters
    val conf = new JobConf(this.getClass)
    conf.setJobName("DescendingOrder")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    //Creating a new time format to append to our output directory
    var timeformat = new SimpleDateFormat("dd-MM-yyyy-hh-mm-ss")
    //Saves the trouble of having to delete the output directory again and again
    //Taking path as a variable here to that we can keep track of where this unsorted file went.
    //helps keep a more user friendly dir structure
    val outpath = outputPath + funcconfig.getString("OutputPath") + "_" + timeformat.format(Calendar.getInstance().getTime)
    FileOutputFormat.setOutputPath(conf, new Path(outpath + "\\unsortedoutput"))
    logger.info(s"Job configurations set. Starting job." + conf.getJobName)
    JobClient.runJob(conf)


    logger.info(s"The main job:" + conf.getJobName + " has ended successfully.")
    val conf2 = new JobConf(this.getClass)
    conf2.setJobName("SortingDescendingOrder")
    conf2.set("fs.defaultFS", "local")
    conf2.set("mapreduce.job.maps", "1")
    conf2.set("mapreduce.job.reduces", "1")
    conf2.setMapOutputKeyClass(classOf[IntWritable])
    conf2.setMapOutputValueClass(classOf[Text])
    conf2.setOutputKeyClass(classOf[Text])
    conf2.setOutputValueClass(classOf[IntWritable])
    conf2.setMapperClass(classOf[SortMap])
    //conf2.setCombinerClass(classOf[SortReduce])
    conf2.setReducerClass(classOf[SortReduce])
    conf2.setInputFormat(classOf[TextInputFormat])
    conf2.set("mapred.textoutputformat.separator", ",")
    conf2.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf2, new Path(outpath + "\\unsortedoutput\\part-00000"))
    //Creating a new time format to append to our output directory
    //Saves the trouble of having to delete the output directory again and again
    FileOutputFormat.setOutputPath(conf2, new Path(outpath + "\\finaloutput"))
    logger.info(s"Job configurations set. Starting job." + conf2.getJobName)
    JobClient.runJob(conf2)



