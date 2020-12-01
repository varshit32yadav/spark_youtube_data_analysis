//package tube.analysis
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}



object Uploader_Video_Count {


  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
  val conf = new SparkConf().setMaster("local[2]").setAppName("Number of Uploaded Video by User")
  val sc = new SparkContext(conf)

  val raw_data = sc.textFile("file:///home/varshit/Videos/SparkProject_Youtube_Analysis-master/youtubedata.txt")


  val user_list = raw_data.filter(line => line.split("\\t").length >1).map(line => (line.split("\\t")(1),1) )

   val video_count_ofuser = user_list.reduceByKey(_+_).sortBy(_._2,ascending = false)

    video_count_ofuser.saveAsTextFile("/home/varshit/Videos/SparkProject_Youtube_Analysis-master/User_Video_Count")
  video_count_ofuser.foreach(println)
    sc.stop()
}
}
