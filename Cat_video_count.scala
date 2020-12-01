//package tube.analysis


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}



object Cat_video_count {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]){

//  val conf = new SparkConf().local("setMaster[3]").setAppName("Count_video_in_Category")
//  val sc = new SparkContext.getOrCreate()

  val youtube_data = sc.textFile("file:///home/varshit/Videos/SparkProject_Youtube_Analysis-master/youtubedata.txt")

   
    val values = youtube_data.filter(line=>line.split("\\t").length>2).map{ line =>
       val lst = line.split("\\t")
      //(video_category ,1 )
       (lst(3),1)
       }

    
   val reducedData = values.reduceByKey(_+_).map{case(x,y)=>(y,x)}.sortByKey(false)

   
    reducedData .saveAsTextFile("/home/varshit/Videos/SparkProject_Youtube_Analysis-master/Category_video_counts")

    reducedData .collect().foreach(println)
    sc.stop()

}
}
