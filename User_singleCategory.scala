//package tube.analysis

import com.sun.media.jfxmedia.locator.LocatorCache.CacheReference
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object User_singleCategory {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("user video in single category")
    val sc = new SparkContext(conf)
    val raw_data = sc.textFile("file:///home/varshit/Videos/SparkProject_Youtube_Analysis-master/youtubedata.txt")

    val userand_Cateory = raw_data.filter(line => line.split("\\t").length > 3).map { x =>
      val video_info = x.split("\\t")
      (video_info(1),video_info(3))
    }
    var category_lst = List[Any]()
    var category_set = Set[Any]()


  val user_category_grp = userand_Cateory.groupByKey()

    val user_category_detail = user_category_grp.map{ case(userId,category_Buffer) =>

      category_lst = Nil
      category_Buffer.foreach{ category =>     
      category_lst = category :: category_lst    
    }

      category_set = category_lst.toSet 
   
      (userId,category_set,category_set.size,if(category_set.size==1) true else false)

    }

   
    val user_upload_onecategory = user_category_detail.filter( record => record._4==true)
    
    val user_upload_mulcategory = user_category_detail.filter(record => record._4==false)

    println("Single Category User")
    user_upload_onecategory.foreach(println)

    println("Multiple Category User")
    user_upload_mulcategory.foreach(println)

    
    sc.stop()

}
}
