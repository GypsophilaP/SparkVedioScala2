package SparkVedioScala2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Gypsophila on 2017/8/31.
  */
object LocalFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalFile").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://node1:9000/spark.txt")
    val lengths = lines.map( length => length.length)
    val res = lengths.reduce(_+_)
    println(res)
  }
}
