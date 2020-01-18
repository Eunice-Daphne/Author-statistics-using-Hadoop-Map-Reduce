import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

/**
 * The MapReduce Driver class creates a job instance for the Map Reduce Task defining the mapper class, reducer class, Input and Output key-value formats, Input and Output File paths.
 * The output file contains all the statistics in a single file which can be later converted to csv file.
  */
object MapReduceDriver {

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    logger.debug("Configure Map Reduce Task")

    val conf = ConfigFactory.load("TagConf")

    val configuration = new Configuration
    //Load the XML input start tags
    configuration.set("xmlinput.start", conf.getString("Start_tags"))
    //Load the XML input stop tags
    configuration.set("xmlinput.end", conf.getString("End_tags"))
    //Set the text ouput seperator as ',' to output to convert to CSV file
    configuration.set("mapred.textoutputformat.separator", ",")

    val job = Job.getInstance(configuration, "AllStatistics")

    job.setJarByClass(this.getClass)
    //Set the Mapper Class
    job.setMapperClass(classOf[MapperBin])
    //Set the input format
    job.setInputFormatClass(classOf[XmlInputFormat])
    //Set the Mapper output Key-value format
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    //job.setCombinerClass(classOf[ReducerBin])
    //Set the Reducer Class
    job.setReducerClass(classOf[ReducerBin])
    //Set the Reducer output Key-Value format
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    //Get the input and output file paths
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    logger.debug("Wait for job completion")
    System.exit(if (job.waitForCompletion(true)) 0 else 1)


  }
}