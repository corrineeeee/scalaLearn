package work

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  *
  * @ClassName SensitiveWord2C
  * @Author Corrine;-)
  * @Date 2020/2/3 15:28
  */

object SensitiveWord2C {

  def main(args: Array[String]) {

    import org.apache.spark.sql.SparkSession
    val sparkSession = SparkSession.builder.appName("SensitiveWord2C task").getOrCreate
    val sc = sparkSession.sparkContext

    val dataTime = args(0)

    val task_type_id = "164"
    //火影敏感词问题类型
    val problemTypeId_kr = "175"
    //大额敏感词问题类型
    val problemTypeId_charge = "180"

    val result = sparkSession.sql(
      """
        |select distinct tt1.user_id,tt1.scene_id,tt1.content,tt1.create_time1,nvl(tt2.name,tt1.agent_id) as agent_id,tt1.remark_name,tt1.is_send,tt1.igid,tt1.task_type_id,tt1.problem_type_id
        |from
        |(
        |	select distinct t1.user_id,t2.scene_id,t1.content,t1.create_time1,t1.agent_id,t2.remark_name,t1.is_send,t2.igid,t2.task_type_id,t2.problem_type_id
        |	from
        |	(
        |		select  user_id,create_time,session_id,content,create_time as create_time1,agent_id,is_send
        |		from messagelist
        |		where  from_unixtime(unix_timestamp(create_time),'yyyyMMdd')  = %s and user_id is not null and agent_id != 0
        |	)t1
        |	join
        |	(
        |   select /*+mapjoin(t3)*/ t3.task_type_id,t3.problem_type_id,t3.igid,t4.id,t4.scene_id,t4.remark_name
        |     from
        |         (
        |           select task_type_id,problem_type_id,scene_id,igid
        |             from sceneidlist
        |            where task_type_id = %s and problem_type_id in (%s,%s)
        |            group by task_type_id,problem_type_id,scene_id,igid
        |         ) t3
        |    join
        |         (
        |           select id,scene_id,remark_name
        |		          from sessionlist
        |            group by id,scene_id,remark_name
        |         ) t4
        |     on t3.scene_id = t4.scene_id
        |     group by t3.task_type_id,t3.problem_type_id,t3.igid,t4.id,t4.scene_id,t4.remark_name
        |	)t2
        |	on t1.session_id = t2.id
        |)tt1
        |left join
        |namelist  tt2
        |on tt1.agent_id = tt2.agent_id
      """.format(dataTime, task_type_id, problemTypeId_kr, problemTypeId_charge).stripMargin).persist(StorageLevel.MEMORY_AND_DISK)

    val contenttotal = result.rdd.map(rec => {
      val user_id = rec.apply(0).toString
      val content = rec.apply(2).toString
      val create_time = rec.apply(3).toString
      val is_send = rec.apply(6).toString
      val igid = rec.apply(7).toString
      val task_id = rec.apply(8).toString
      val problem_id = rec.apply(9).toString
      println("contenttotal:", user_id, content, create_time, is_send, igid, task_id, problem_id)
      (user_id, create_time, content, is_send, igid, task_id, problem_id)
    })

    //火影敏感词
    val sensitiveWord_kr = Array("投诉", "弃坑", "举报", "合区", "建议")
    //大额敏感词
    val sensitiveWord_charge = Array("大额", "充值", "代充", "充钱")

    val WordList = Array((task_type_id, problemTypeId_kr, sensitiveWord_kr), (task_type_id, problemTypeId_charge, sensitiveWord_charge))

    //广播变量 过滤并拉取上下文
    val contentresult: Broadcast[Map[(String, String, String, String), (List[(String, String, String)])]] = sc.broadcast(getContext(contenttotal, WordList))
    contentresult.value.toList.foreach(f => println("contentresult:" + f.toString()))

    result.rdd.map(rec => {
      val user_id = rec.apply(0).toString
      val scene_id = rec.apply(1).toString
      val agent_id = rec.apply(4).toString
      val remark_name = rec.apply(5).toString
      val igid = rec.apply(7).toString
      val task_id = rec.apply(8).toString
      val problem_id = rec.apply(9).toString
      //val content = contentresult.value.getOrElse(user_id, "NULL").toString
      (user_id, scene_id, agent_id, remark_name, igid, task_id, problem_id)
    }).distinct.filter(rec => {
      contentresult.value.getOrElse((rec._1, rec._5, rec._6, rec._7), "NULL").toString != "NULL"
    })
      .foreach(rec => {
        val user_id = rec._1
        val scene_id = rec._2
        val agent_id = rec._3
        val remark_name = rec._4
        val igid = rec._5
        val task_id = rec._6
        val problem_id = rec._7
        val content = contentresult.value.getOrElse((user_id, igid, task_id, problem_id), List("NULL", "NULL", "NULL"))

        println("result:", user_id, scene_id, agent_id, remark_name, igid, task_id, problem_id)

        //val content = contentresult.value.getOrElse(user_id,"NULL").toString
        //火影忍者OL
        //println("UIN:%s -- SCENEID:%s -- AGENTID:%s --REMARKNAME:%s -- CONTENG:%s".format(user_id, scene_id,agent_id,remark_name,content))
        println(
          """
            |UIN:%s
            |SCENEID:%s
            |AGENTID:%s
            |CONTENG:%s
            |IGID:%s
            |TASKID:%s
            |PROBLEM:%s
            |""".format(user_id, scene_id, agent_id, content, igid, task_id, problem_id).stripMargin)
        //发送工单操作
      })


  }

  /**
    * 匹配操作台关键词 并且拉取上下文各十条
    *
    * @param value
    * @return
    */
  def getContext(value: RDD[(String, String, String, String, String, String, String)], wordlist: Array[(String, String, Array[String])]): Map[(String, String, String, String), (List[(String, String, String)])] = {
    println("start getContext")

    //方法 seekindex 拉取上下文各十条逻辑
    def seekindex(index: Int, list: List[(String, String, String)]): List[(String, String, String)] = {
      var start = 0
      var end = 0
      val length = list.length - 1
      val size = 20
      println("index:" + index, "length:" + length)
      start = index
      end = index
      for (elem <- 1 to size / 2) {
        if (start > 0) start = start - 1
      }

      for (elem <- 1 to size / 2) {
        if (end < length) end += 1
      }
      println(length, start, index, end)
      var resultlist = new ListBuffer[(String, String, String)]
      for (i <- start to end) {
        resultlist += list.apply(i)
        //println("add to" , list.apply(i))
      }
      for (elem <- resultlist) {
        println("resultlist:" + elem)
      }
      resultlist.toList
    }


    val result = value.map(row => {
      val userid = row._1.toString
      val datatime = row._2.toString
      val content = row._3.toString
      val is_send = row._4.toString
      val igid = row._5.toString
      val task_id = row._6.toString
      val problem_id = row._7.toString
      ((userid, igid, task_id, problem_id), (datatime, content, is_send))
    }).groupByKey().mapValues(row => {
      val list = row.toList.sortBy(f => f._1) //.map(f=>f._2)

      list
    }).filter(f => {
      //筛选关键词
      var flag = false

      for (wlist <- wordlist) {
        if (f._1._3 == wlist._1 && f._1._4 == wlist._2) {
          for (elem <- f._2) {
            for (word <- wlist._3) {
              if (elem._2.contains(word)) {
                //是否是玩家发送的 0是1不是
                if (elem._3.contains("0")) {
                  flag = true
                } else {
                  flag
                }
              } else {
                flag
              }
            }
          }
        }
      }
      //      for (elem <- f._2) {
      //        for (word <- wordlist) {
      //          //if (elem._2.contains("反馈") || elem._2.contains("稍等") || elem._2.contains("核实") || elem._2.contains("提交") || elem._2.contains("记录")) {
      //          if (elem._2.contains(word)) {
      //            //是否是玩家发送的 0是1不是
      //            if (elem._3.contains("0")) {
      //              flag = true
      //            } else {
      //              flag
      //            }
      //          } else {
      //            flag
      //          }
      //        }
      //      }
      flag
    }).map(f => {
      var y = 0
      var taskid = ""
      var problemid = ""

      for (wlist <- wordlist) {
        var wtaskid = wlist._1.toString
        var wproblemid = wlist._2.toString

        if (f._1._3 == wtaskid && f._1._4 == wproblemid) {
          for (elem <- f._2) {
            //返回关键词所在的那一列的索引
            for (word <- wlist._3) {
              //if (( elem._2.contains("反馈") || elem._2.contains("稍等") || elem._2.contains("核实") || elem._2.contains("提交") || elem._2.contains("记录")) && elem._3.contains("1")){
              if (elem._2.contains(word) && elem._3.contains("0")) {
                val i: Int = f._2.indexOf(elem)
                println("find -> ", i, elem)
                y = i
                taskid = wtaskid
                problemid = wproblemid
              }
              y
            }
          }
        }

      }

      val resultlist1: List[(String, String, String)] = seekindex(y, f._2)
      var map = Map[(String, String, String, String), (List[(String, String, String)])]()
      map += ((f._1._1, f._1._2, taskid, problemid) -> (resultlist1))
      map
    }).persist(StorageLevel.MEMORY_AND_DISK).collect().flatten.toMap
    //.foreach(f=>println("final:"+f._1,f._2.toString()))

    result
  }
}
