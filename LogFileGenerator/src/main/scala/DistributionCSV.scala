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

object MapReduceProgram:
  val config: Config = ConfigFactory.load("application.conf")
  val funcconfig = config.getConfig("randomLogGenerator.functionalityconfigs.mapreducetocsv")
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      //val line: String = value.toString
      println("config=" +config)
      println("funcconfig=" +funcconfig)
      val injectedpattern = Pattern.compile(config.getString("randomLogGenerator.Pattern"))
      val userdefinedpattern = Pattern.compile(funcconfig.getString("FindOccurrenceOf"))
      val timeinterval = value.toString.substring(0,5)
      val matchinjected = injectedpattern.matcher(value.toString)
      val matcheduserdefined = userdefinedpattern.matcher(value.toString)
      println("AAAAA")
      println("timeinterval= "+ timeinterval)
      println("matchinjected= "+ matchinjected)
      println("matcheduserdefined= "+ matcheduserdefined)
      if(matchinjected.find() && matcheduserdefined.find())
      {
        val loglevel = matcheduserdefined.group()
        println("AAAAA found: "+loglevel)
        word.set(timeinterval + ":00 " + loglevel)
        output.collect(word, one)
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.foldLeft(0)(_+_.get)
      output.collect(key, new IntWritable(sum))
  //val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
  //output.collect(key, new IntWritable(sum.get()))

  @main def runMapRed(inputPath: String, outputPath: String) =
    require(!inputPath.isEmpty && !outputPath.isEmpty)
    println(inputPath)
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("WordCount")
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
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
