package SparkVedioScala2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Gypsophila on 2017/8/30.
  */
object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10 , 1)
    println(rdd.reduce(_+_))
  }
}
