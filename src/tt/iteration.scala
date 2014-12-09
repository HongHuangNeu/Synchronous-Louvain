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

object iteration {

  def main(args: Array[String]): Unit = {
    println("real run"+math.log(2));
    println("scala works")
    println("Spark initialization")
    /*
     * Spark initialization
     * */
    val N=Setting.numOfNodes
    val conf = new SparkConf().setAppName("hello").setMaster("local")
    val sc = new SparkContext(conf)
//manipulate community.dat
    val textFile1 = sc.textFile(Setting.graphHome+"/community.dat")
    val clustering1 = textFile1.flatMap(e => {
      val str = new StringTokenizer(e, "\t");
      val nodeId = str.nextToken().trim().toLong
      val communityId = str.nextToken().trim().toLong
      Array((communityId, 1L))
    })
    val join1 = textFile1.flatMap(e => {
      val str = new StringTokenizer(e, "\t");
      val nodeId = str.nextToken().trim().toLong
      val communityId = str.nextToken().trim().toLong
      Array((nodeId, communityId))
    })
    //compute the number of vertices for each cluster
    val clustering1Count = clustering1.reduceByKey(_ + _)
    val cluster1Map=clustering1Count.collectAsMap
    
//manipulate the community I produce    
    val textFile2 = sc.textFile(Setting.graphHome+"/graph_node2comm_level2")
    val clustering2 = textFile2.flatMap(e => {
      val str = new StringTokenizer(e, " ");
      val nodeId = str.nextToken().toLong
      val communityId = str.nextToken().toLong
      Array((communityId, 1L))
    })
    val join2 = textFile2.flatMap(e => {
      val str = new StringTokenizer(e, " ");
      val nodeId = str.nextToken().toLong
      val communityId = str.nextToken().toLong
      Array((nodeId, communityId))
    })
    //compute the number of vertices for each cluster
    val clustering2Count = clustering2.reduceByKey(_ + _)
    val cluster2Map=clustering2Count.collectAsMap
    
    val join=join1.join(join2).values
    
    val tmp=join.map{ case (communityX, communityY) =>{((communityX, communityY),1L)}}
    val ijCount=tmp.reduceByKey(_+_)
    val ijMap=ijCount.collectAsMap
    var sum=0.0
    ijMap.foreach(f=>println(f))
    ijMap.foreach(f=>{
       sum=sum+f._2.toDouble*math.log(f._2.toDouble*N.toDouble/(cluster1Map(f._1._1).toDouble*cluster2Map(f._1._2).toDouble))
    })
    
    var sum1=0.0
    cluster1Map.foreach(f=>{
      sum1=sum1+f._2.toDouble*math.log(f._2.toDouble/N.toDouble)
    })
    cluster1Map.foreach(f=>println(f))
    var sum2=0.0
    cluster2Map.foreach(f=>{
      sum2=sum2+f._2.toDouble*math.log(f._2.toDouble/N.toDouble)
    })
    
    val NMI=(-2)*sum/(sum1+sum2)
    println("NMI"+NMI)
    println(sum)
  }
  
  

}