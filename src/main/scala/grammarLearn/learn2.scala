package grammarLearn

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * @AUTHOR CORRINE
 * @DATE 2020/1/13 14:54
 * @VERSION 1.0
 */
object learn2 {
  def main(args: Array[String]): Unit = {
    val origarray =  List(("corrine","c","100"),("corrine","c","1099"),("corrine","c","199"),("yyy","1","18"),("yyy","1","20"))

//    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("learn2"))
//    val value = sc.parallelize(origarray.toList)

    origarray.map(f=>{
      (f._1,(f._2,f._3))
    }).groupBy(f=>f._1).mapValues(f=>{
     val list =  new ListBuffer[String]
      f.foreach(row=>list += row._2._2.toString)
//      f.foreach(row=>{
//        (row._1,row._2._1,list)
//      })
      (f.apply(0)._2._1,list)
    }).foreach(rec=>println(rec._1.toString,rec._2.toString))

  }
}
