package work

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @AUTHOR CORRINE
 * @DATE 2019/12/11 16:04
 * @VERSION 1.0  拉取上下文逻辑
 */
object SensitiveWord {

  case class usercontent (userid:String,datatime:Int,content:String,is_send:String)

  def main(args: Array[String]): Unit = {
    //%%转义符
//    val datatime = "20191010"
//    val str =
//      """
//        |abc = %s
//        |like '%%you%%'
//        |""".format(datatime).stripMargin
//    println(str)

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sensitive word")
    val sc = new SparkContext(conf)

    val user1 = new usercontent("corrine", 20191010,"hello0",is_send = "1")
    val user2 = new usercontent("corrine",20191011,"举报1",is_send = "0")
    val user3 = new usercontent("corrine",20191012,"hello2",is_send = "1")
    val user15 = new usercontent("corrine",20191013,"记录3",is_send = "1")
    val user4 = new usercontent("kolin",20191013,"hello3",is_send = "0")
    val user5 = new usercontent("kolin",20191012,"反馈2",is_send = "0")
    val user6 = new usercontent("kolin",20191011,"hello1",is_send = "1")
    val user7 = new usercontent("cathy",20191016,"hello6",is_send = "1")
    val user8 = new usercontent("cathy",20191013,"hello3",is_send = "1")
    val user9 = new usercontent("cathy",20191014,"举报4",is_send = "0")
    val user10 = new usercontent("cathy",20191015,"hello5",is_send = "1")
    val user11 = new usercontent("cathy",20191018,"hello8",is_send = "1")
    val user12 = new usercontent("cora",20191011,"hello1",is_send = "1")
    val user13 = new usercontent("cora",20191013,"hello3",is_send = "1")
    val user14 = new usercontent("cora",20191012,"hello2",is_send = "1")
    val user17 = new usercontent("cathy",20191018,"hello8",is_send = "0")
    val user18 = new usercontent("cathy",20191018,"hello8",is_send = "1")
    val user19 = new usercontent("cathy",20191018,"hello8",is_send = "1")
    val user20 = new usercontent("cathy",20191018,"hello8",is_send = "1")
    val user21 = new usercontent("cathy",20191018,"提交8",is_send = "0")
    val user22 = new usercontent("cathy",20191018,"hello8",is_send = "1")
    val user23 = new usercontent("cathy",20191018,"hello8",is_send = "1")
    val user24 = new usercontent("cathy",20191018,"hello8",is_send = "1")
    val user25 = new usercontent("cathy",20191020,"记录20",is_send = "1")
    val user26 = new usercontent("cathy",20191021,"hello8",is_send = "1")
    val user27 = new usercontent("cathy",20191021,"hello8",is_send = "1")
    val user28 = new usercontent("cathy",20191021,"hello8",is_send = "1")

    val list = new ListBuffer[usercontent]
    list += user1
    list += user2
    list += user3
    list += user4
    list += user5
    list += user6
    list += user7
    list += user8
    list += user9
    list += user10
    list += user11
    list += user12
    list += user13
    list += user14
    list += user15
    list += user18
    list += user17
    list += user19
    list += user20
    list += user21
    list += user22
    list += user23
    list += user24
    list += user25
    list += user26
    list += user27
    list += user28

    val rddvalue = sc.parallelize(list)
    getContext(rddvalue)

    //废弃尝试
//    val index = 10
//    var start = 0
//    var end = 0
//    var take = 0
//    val size = 20
//    val length = 20-1
//    start = index
//    end = index
//    while(start>0 && take<size/2){
//      take += 1
//      start = start -1
//    }
//    println(start)

//    start = index
//    end = index
//    for (elem <- 1 to size/2){
//      if (start>0) start = start -1
//    }
//
//
//    for (elem <- 1 to size/2){
//      if (end < length) end += 1
//    }

//    take = 0
//    while(end>0 && take<length && take < size/2)
//    {
//      take += 1
//      end += 1
//    }
//    println(start,end)
//
//    rddvalue.map(rec=>{
//      val userid = rec.userid
//      userid
//    }).distinct.foreach(rec=>println(rec))

  }

  def getContext (value:RDD[usercontent]):Unit ={

    //找到目标的上下十条的所有有效索引
    def seekindex(index:Int , list: List[(Int,String,String)]): List[(Int,String,String)] = {
      var start = 0
      var end = 0
      val length = list.length - 1

      start = index
      end = index
      for (elem <- 1 to 10){
        if (start>0) start = start -1
      }


      for (elem <- 1 to 10){
        if (end < length) end += 1
      }
      //废弃尝试:-( 过于复杂 并且没有效果
//      if (length >= index*2 && index >= 10 ){
//        index1 = index-10
//        index2 = index+10
//      }else if (length >= index*2 && index <10){
//        if (length>=index+10){
//          index1 = 0
//          index2 = index + 10
//        }else if (length<index+10){
//          index1 = 0
//          index2 = length-1
//        }
//      }else if (length < index*2 && index>=10){
//        if (length>=index+10){
//          index1 = index -10
//          index2 = index +10
//        }else if (length<index+10){
//          index1 = index -10
//          index2 = length-1
//        }
//      }else if (length < index*2 && index <10){
//        index1 = 0
//        index2 = length-1
//      }

      println(length,start,index,end)
      var resultlist = new ListBuffer[(Int,String,String)]
      for(i  <- start to end){
        resultlist+=list.apply(i)
        //println("add to" , list.apply(i))
      }
      //for (elem <- resultlist) {println("resultlist:" + elem)}
      resultlist.toList
    }


    val result = value.map(row => {
      val userid = row.userid.toString
      val datatime = row.datatime.toInt
      val content = row.content.toString
      val is_send = row.is_send.toString
      (userid,(datatime,content,is_send))
    }).groupByKey().mapValues(row=>{
      val list = row.toList.sortBy(f=>f._1)//.map(f=>f._2)

      list
    }).filter(f=>{
      var flag = false
      for (elem <- f._2) {
        val strings = Array("举报","反馈","稍等","核实","提交","记录")
        //if (elem.contains("举报") || elem.contains("反馈") || elem.contains("稍等") || elem.contains("核实") || elem.contains("提交") || elem.contains("记录")){
        //if (elem._2.contains("举报") || elem._2.contains("反馈") || elem._2.contains("稍等") || elem._2.contains("核实") || elem._2.contains("提交") || elem._2.contains("记录")){
        for (elem1 <- strings) {
          if (elem._2.contains(elem1)) {
            if (elem._3.contains("1")) {
              flag = true
            } else {
              flag
            }
            //flag = true
          } else {
            flag
          }
        }
      }
      flag
    }).map(f=>{
      var y = 0
      for (elem <- f._2) {
        if ((elem._2.contains("举报") || elem._2.contains("反馈") || elem._2.contains("稍等") || elem._2.contains("核实") || elem._2.contains("提交") || elem._2.contains("记录")) && elem._3.contains("1")){
          val i: Int = f._2.indexOf(elem)
          //println("find -> ", i, elem)
          y = i
        }
        y
      }
      val resultlist1: List[(Int,String,String)] = seekindex(y, f._2)
      var map  = Map[String,List[(Int,String,String)]]()
      map += (f._1->resultlist1)
      map
    }).collect().flatten.toMap//.cache()
      .foreach(f=>println("final:"+f._1,f._2.toString()))

    result

  }

}
