//package tube.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object maximum_rated_video {


  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Top_Rated_Video")
    val sc = new SparkContext(conf)

    val youtube_raw_data = sc.textFile("file:///home/varshit/Videos/SparkProject_Youtube_Analysis-master/youtubedata.txt")

   
    val values = youtube_raw_data.filter(line => line.split("\\t").length > 7).map { line =>
      val lst = line.split("\\t")
      (lst(0), lst(6).toFloat,lst(3))
    }


    val ratingAsKey=values.map{case(x,y,z)=>(y,x,z)}

   
    val top10 = ratingAsKey.sortBy(_._1,ascending = false).take(10)

  
    val top10Rdd = sc.parallelize(top10,1)
    top10Rdd.foreach(println)

  
     top10Rdd.saveAsTextFile("/home/varshit/Videos/SparkProject_Youtube_Analysis-master/topRatedVideos")

sc.stop()
  }
}
