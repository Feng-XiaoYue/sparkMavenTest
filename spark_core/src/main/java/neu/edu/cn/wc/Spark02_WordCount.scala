package neu.edu.cn.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和Spark框架的连接
    //JDBC:Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作
    //1.读取文件，获取一行一行的数据
    val lines: RDD[String] = sc.textFile("{datas/*}")

    val word: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = word.map(
      word => (word, 1)
    )

    val wordTOCount = wordToOne.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordTOCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop()
  }

}
