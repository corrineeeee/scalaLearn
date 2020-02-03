import java.text.SimpleDateFormat

import javax.print.DocFlavor.STRING
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
//import java.text.SimpleDateFormat


object test1 {

  case class LossUser(datatime:Int,game_id:Int,openid:String,classification_info:String)


  def main(args: Array[String]): Unit = {
    //    val strings = getLastWeek("20190903")
    //    for (elem <- strings) {
    //      //println(elem)
    //    }
    //
    //    val time = "2019-01-01 29:23:21"
    //    println(time)
    //    val newtime = time.substring(0,10).replace("-","")
    //    println(newtime)

    //    val string1 = "1252"
    //    val string2 = "1252"
    //    println(string1 != string2)
    //
    //    val strings = getLastMonth("20191030")
    //    println(strings)

    val lossTalkTable = List("1828","1574","2144","1759")
    for  (elems <- lossTalkTable){
      val tableName = "tp_text_loss_classification_"+elems
      println(tableName)
    }

    val conf = new SparkConf().setMaster("local").setAppName("RDDTest")
    val sc = new SparkContext(conf)

    val user1 = new LossUser(20191001,1828,"1BE8ECA62F88E6FC9AFEE28805C9C7D6","挫败:0;出行:0;厌倦:1;没时间:0;代玩:0;回流观望:0;换游戏:0")
    val user2 = new LossUser(20191020,1828,"1BE8ECA62F88E6FC9AFEE28805C9C7D6","挫败:0;出行:0;厌倦:0;没时间:0;代玩:0;回流观望:0;换游戏:0")
    val user3 = new LossUser(20191018,1828,"1BE8ECA62F88E6FC9AFEE28805C9C7D6","挫败:7;出行:0;厌倦:2;没时间:0;代玩:8;回流观望:0;换游戏:0")
    val user4 = new LossUser(20191019,1828,"1D1DF4C7C8B0111B25F0FB6382F63EF3","挫败:0;出行:0;厌倦:0;没时间:0;代玩:0;回流观望:0;换游戏:0")
    val user5 = new LossUser(20191001,1828,"1D1DF4C7C8B0111B25F0FB6382F63EF3","挫败:1;出行:0;厌倦:0;没时间:0;代玩:0;回流观望:0;换游戏:0")

    var list = new ListBuffer[LossUser]
    list += user1
    list += user2
    list += user3
    list += user4
    list += user5

    var testapply: mutable.HashMap[String, (String, String)] = new mutable.HashMap[String,(String,String)]()
    testapply.put("1D1DF4C7C8B0111B25F0FB6382F63EF3",("1828","111"))
    testapply.put("1D1DF4C7C8B0111B25F0FB6382F63EF3",("1828","111"))
    testapply.put("1BE8ECA62F88E6FC9AFEE28805C9C7D6",("1828","112"))

    val (gid,ccc) = testapply.getOrElse("111",("null","null"))
    val (gid2,aaa) = testapply.getOrElse("111",("null","null"))
    val (gid3,ccbbbc) = testapply.getOrElse("1BE8ECA62F88E6FC9AFEE28805C9C7D6",("null","null"))

    println(ccc)
    println(aaa)
    println(ccbbbc)

    val value = sc.parallelize(list)
    val users = value.collect().take(5).foreach(rec=>{
      println(rec.classification_info)
    })

    //
    //    val users = value.map(rec => {
    //      val openid = rec.openid
    //      val datatime = rec.datatime
    //      val classification_info = rec.classification_info
    //
    //      val pattern = new Regex("(\\d+)")
    //      val classification= (pattern findAllIn rec.classification_info).mkString("").toInt
    //
    //      (openid.toString,(datatime.toInt,classification_info.toString,classification.toInt))
    //    }).filter(rec => {
    //      rec._2._3 > 0
    //    }).groupByKey().mapValues(rec => {
    //      rec.toList.sortBy(f => {f._1}).reverse.take(1)
    //    })
    //
    //    for (elem <- users) {println(elem)}

    getLossTalkTap(sc, list)



  }
  def getLossTalkTap (sc : SparkContext,list: ListBuffer[LossUser])={
    //val value = tdw.table(tableName,Seq("p_"+ dataTime))
    val value = sc.parallelize(list)

    val finalvalue = value.map(rec => {
      val openid = rec.openid
      val datatime = rec.datatime
      val classification_info = rec.classification_info

      val pattern = new Regex("(\\d+)")
      val classification = (pattern findAllIn rec.classification_info).count(x => x.equals("0") == false)

      (openid.toString, (datatime.toString, classification_info.toString, classification))
    }).filter(rec => {
      rec._2._3 > 0
    }).groupByKey().mapValues(rec => {
      val tuples = rec.toList.sortBy(f => f._1).reverse.take(1)
      //val openid = tuples.apply(0)
      val game_id = tuples(0)._3
      val datatime = tuples(0)._1
      val classification_info = tuples(0)._2.toString
      //val tuple1 = tuples(1)
      ( game_id,datatime,classification_info)
    }).map(rec => {
      val openid = rec._1
      val game_id = rec._2._1
      val datatime = rec._2._2
      val classification_info =rec._2._3
      (openid, (datatime, game_id,classification_info))
    })




    for (elem <- finalvalue) {println(elem)}
    finalvalue

  }




  //  def getLastWeek(date: String) : Seq[String] = {
  //    var result = Seq[String]()
  //    import java.util.Calendar
  //    val cal = Calendar.getInstance
  //    val firstDay = new SimpleDateFormat("yyyyMMdd").parse(date)
  //    val formatter = new SimpleDateFormat("yyyyMMdd");
  //    cal.setTime(firstDay)
  //    cal.add(Calendar.DAY_OF_MONTH, -7)
  //    val begindate = formatter.format(cal.getTime)
  //    cal.add(Calendar.DAY_OF_MONTH, 6)
  //    //cal.add(Calendar.DATE, -1)
  //    val enddate = formatter.format(cal.getTime)
  //    //println(begindate)
  //    //println(enddate)
  //    for( i <- begindate.toInt to enddate.toInt){
  //      result = result :+ i.toString
  //    }
  //    result
  //  }

  def getLastMonth(date: String) : Seq[String] = {
    var result = Seq[String]()
    import java.util.Calendar
    val cal = Calendar.getInstance
    val firstDay = new SimpleDateFormat("yyyyMMdd").parse(date.substring(0,6) + "01")
    val formatter = new SimpleDateFormat("yyyyMMdd");
    cal.setTime(firstDay)
    cal.add(Calendar.MONTH, -1)
    val begindate = formatter.format(cal.getTime)
    cal.add(Calendar.MONTH, 1)
    cal.add(Calendar.DATE, -1)
    val enddate = formatter.format(cal.getTime)
    println(begindate)
    println(enddate)
    for( i <- begindate.toInt to enddate.toInt){
      result = result :+ i.toString
    }
    result
  }

  def getDateDelta(datetime : String, day : Int)  = {
    //ScalaComm.getDateTimeFormatStrFromTimeStamp("yyyyMMdd", (ScalaComm.getTimeStampFromStr("yyyyMMdd", datetime) + (86400L * day)).toInt)
  }


}
