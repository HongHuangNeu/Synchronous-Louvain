package tt
import org.apache.spark.SparkContext
import java.lang.Math
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import scala.collection._
import java.util.StringTokenizer;
object runner {
def main(args: Array[String]): Unit = {
   println("real run");
    println("scala works")
    println("Spark initialization")
    /*
     * Spark initialization
     * */
    val conf = new SparkConf().setAppName("hello").setMaster("local")
    val sc=new SparkContext(conf)
    //println("program parameter is "+args(0))
  println("test")
  val path="/home/honghuang/Documents/benchmark/weighted_networks/network.dat"
  val initialGraph=moreLevel.createLouvainGraph(path, sc, 1000L)
  val result=moreLevel.louvainOneLevel(initialGraph, sc)
  
}
}