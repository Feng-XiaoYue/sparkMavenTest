package ThreadTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class ThreadExample extends Thread{
  override def run(){

    if (this.getName().equals("Thread_1"))
      task("{datas_1/*}")
    else
      task("{datas_2/*}")

    Thread.sleep(500)

  }

  def task(path: String){
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作
    //1.读取文件，获取一行一行的数据
    val lines: RDD[String] = sc.textFile(path)

    //2.将一行数据进行拆分，形成一个一个的单词（分词）
    val word: RDD[String] = lines.flatMap(_.split(" "))

    //3.将数据根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = word.groupBy(word => word)

    //4.将分组之后的数据进行转换
    val wordTOCount = wordGroup.map{
      case (word, list)=>
        (word,list.size)
    }
    //5.将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordTOCount.collect()
    array.foreach(println)
    //TODO 关闭连接
    sc.stop()
  }

}




object Threadtest1 {
  def main(args: Array[String]): Unit = {
    var t1 = new ThreadExample()
    var t2 = new ThreadExample()
    t1.setName("Thread_1")
    t2.setName("Thread_2")
    t1.start()
    t1.join()
    t2.start()


  }

}
