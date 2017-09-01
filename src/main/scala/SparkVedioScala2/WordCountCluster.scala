import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Gypsophila on 2017/8/30.
  */
object WordCountCluster{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://node1:7077")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://node1:9000/spark.txt",1)
    val words = lines.flatMap { line => line.split(" ")}
    val pairs = words.map{ word => (word,1)}
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.foreach( wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times."))
  }
}
