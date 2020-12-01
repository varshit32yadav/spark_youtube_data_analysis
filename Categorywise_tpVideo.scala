//package analysis.tube
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


  
  
object Categorywise_tpVideo {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Count_video_in_Category")
    val sc = new SparkContext(conf)

    val raw_data = sc.textFile("file:///home/varshit/Videos/SparkProject_Youtube_Analysis-master/youtubedata.txt")

   
    val Categories = raw_data.filter(line => line.split("\\t").length > 4).map { x =>
      val cat = x.split("\\t")(3)
      (cat)
    }


    val categories_set = Categories.distinct().collect()
  
    val values = raw_data.filter(line => line.split("\\t").length > 7).map { line =>
      val lst = line.split("\\t")
      (lst(0), lst(6).toFloat,lst(3))
    }

  
   val ratingAsKey=values.map{case(x,y,z)=>(y,x,z)}

    var top10=ratingAsKey.take(0);   
    var cat_top10=ratingAsKey.take(0);  

  
     categories_set.foreach{ x =>
     top10 = ratingAsKey.filter{case(rate,videoid,cats) => cats==x}
                          .sortBy(_._1,ascending = false).take(10)
       //top10.foreach(println)
      cat_top10 = cat_top10.union(top10)

   }

    // Converting tuple map into rdd for storage purpose
   val cat_top10RDD = sc.parallelize(cat_top10,1)
    cat_top10RDD.saveAsTextFile("/home/varshit/Videos/SparkProject_Youtube_Analysis-master/Category_Top10_videos")

    println("--------------------")
    println("----- Top 10 of each Category -------")
    println("--------------------")

    cat_top10.foreach(println)
    sc.stop()

  }}
