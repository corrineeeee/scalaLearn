package grammarLearn

/**
 * @AUTHOR CORRINE
 * @DATE 2019/12/19 15:33
 * @VERSION 1.0
 */
object learn1  {
  def main(args: Array[String]): Unit = {


    def sum1(xs: List[Int]): Int =
      if (xs.isEmpty) 0 else xs.head + sum1(xs.tail)

    val list = List(4, 8, 6, 9)
    val i: Int =
    sum1(list)
    println(i)

    val y:Int = 20000
    val str:String =
      """
        |%s
        |""".format(y).stripMargin
    println(str)

    //map
    val map1 = Map("aaa" -> "AAA", "bbb" -> "BBB", "ccc" -> "ccc")
    val map2 = Map("aaa" -> "CCC", "bbb" -> "BBB", "CCC" -> "ccc")

    val map3 = map1 ++ map2
    map3.foreach(f=>println(f._1,f._2))
  }
}
