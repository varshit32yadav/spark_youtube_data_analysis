//package tube.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Extract_Video_Categories {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {

   val conf = new SparkConf().setMaster("local[3]").setAppName("Extract_Video_Category")
    val sc = new SparkContext(conf)

    val raw_data = sc.textFile("file:///home/varshit/Videos/SparkProject_Youtube_Analysis-master/youtubedata.txt")
    val Categories = raw_data.filter(line => line.split("\\t").length > 4).map { x =>
      val cat = x.split("\\t")(3)
      (cat)
    }

    Categories.distinct().saveAsTextFile("/home/varshit/Videos/SparkProject_Youtube_Analysis-master/Categories")
    Categories.distinct().foreach(println)
    sc.stop()
  }
  }
