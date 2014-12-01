package tt
import scala.util.control.Breaks._
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
    val debug=true
    val conf = new SparkConf().setAppName("hello").setMaster("local")
    val sc=new SparkContext(conf)
    //println("program parameter is "+args(0))
  println("test")
  val path="/home/honghuang/Documents/benchmark/weighted_networks/network.dat"
  //  val path="/home/honghuang/test.dat"
  val initialGraph=moreLevel.createLouvainGraph(path, sc, 1000L)
  var results = new Array[Graph[VertexInfo,Double]](10)
  var input=initialGraph
  var result=initialGraph
  var i=1
  do{
   
  println("the "+i+"run")
   result=moreLevel.louvainOneLevel(input, sc,0.6)
   results(i-1)=result
  val tmp=moreLevel.compressGraph(result,sc)
  
  println("info of new graph")
  tmp.vertices.collect.foreach(f=>println(f))
  tmp.edges.collect.foreach(f=>println(f))
  input=tmp
 
  i=i+1
  
  }while(moreLevel.needMoreLevel(result)&&debug);
  println("in total"+(i-2)+"levels")
  for(i<-0 until i-2)
  {
    val r=results(i)
    println("the results for the"+(i+1)+"level")
    r.vertices.collect.foreach(f=>println(f))
    r.vertices.coalesce(1,true).saveAsTextFile("/home/honghuang/result"+i.toString) 
  }
}
}