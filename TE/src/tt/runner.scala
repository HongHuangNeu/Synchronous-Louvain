package tt
import scala.util.control.Breaks._
import org.apache.spark.SparkContext
import java.lang.Math
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import java.util.regex.Matcher
import java.util.regex.Pattern
import scala.collection._
import java.util.StringTokenizer;
import java.io.FileWriter
object runner {
  def main(args: Array[String]): Unit = {
    println("real run");
    println("scala works")
    println("Spark initialization")
    /*
     * Spark initialization
     * */

    val conf = new SparkConf().setAppName("hello").setMaster("local")
    val sc = new SparkContext(conf)

    println("test")

    val path = Setting.graphHome + "/" + Setting.graphFileName

    val initialGraph = moreLevel.createLouvainGraph(path, sc, Setting.numOfNodes)
    var results = new Array[Graph[VertexInfo, Double]](10)
    var input = initialGraph
    var result = initialGraph
    var i = 1
    do {

      println("the " + i + "run")
      result = moreLevel.louvainOneLevel(input, sc, 0.6)
      results(i - 1) = result
      val tmp = moreLevel.compressGraph(result, sc)

      Logger.writeLog("info of new graph")
      tmp.vertices.collect.foreach(f => Logger.writeLog(f.toString))
      tmp.edges.collect.foreach(f => Logger.writeLog(f.toString))
      input = tmp

      i = i + 1

    } while (moreLevel.needMoreLevel(result) && (!Setting.oneLevel));
    Logger.writeLog("in total" + (i - 2) + "levels")
    for (i <- 0 until i - 2) {
      val r = results(i)
      Logger.writeLog("the results for the" + (i + 1) + "level")
      r.vertices.collect.foreach(f => Logger.writeLog(f.toString))
      val fw = new FileWriter(Setting.graphHome + "/result" + i.toString + ".dat")

      r.vertices.coalesce(1, true).saveAsTextFile(Setting.graphHome + "/result" + i.toString)
    }
    Logger.close
  }
}