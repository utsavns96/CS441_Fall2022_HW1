/*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
import scala.io.Source
import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*



object MapReduce:
  //class Map extends Mapper[Object,Text,Text,IntWritable]:
  //  private final val one = new IntWritable(1)
  //  private val word = new Text()
    //private final timeinterval = new

    @main def runMapRed(input: String, output: String) =
      val bufferedSource = Source.fromFile("LogFileGenerator.2022-09-19.log")
      //for (line <- bufferedSource.getLines) {
      val v = bufferedSource.getLines()
      v.foreach{
        println
      }
      bufferedSource.close
*/

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
import scala.io.Source
import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*


object MapReduce:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //val line1:Array[String] = value.toString.split(" ")
      //println("Array: "+ line1.toList)
      //line.split(" ").foreach { token =>
      val line: String = value.toString
      line.split(" ").foreach { token =>
        word.set(token)
        output.collect(word, one)
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  @main def runMapRed(inputPath: String, outputPath: String) =
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


