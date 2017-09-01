package SparkVedioScala2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Gypsophila on 2017/9/1.
  */
object LineCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://node1:9000/spark.txt")
    val linesCount = lines.map( line => (line , 1))
    val res = linesCount.reduceByKey(_+_)
    res.foreach(lincount => println(lincount._1 + lincount._2))
  }
}
