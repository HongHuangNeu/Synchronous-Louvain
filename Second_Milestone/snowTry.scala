package sample
import scala.collection.mutable.Set
import scala.collection.mutable.BitSet
import scala.util.control._
import org.apache.spark.graphx.Graph
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
import java.util.StringTokenizer
import java.io.IOException
import java.io.FileWriter
import scala.collection.mutable.ArrayBuffer
object snowTry {
def main(args: Array[String]): Unit = {
  var selectedNode=BitSet.empty
  var neighbours=BitSet.empty
  val requiredSize=10000
 val numOfNodes=100000L
   val conf = new SparkConf().setAppName("hello").setMaster("local").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g")
    //System.setProperty("spark.executor.memory", "3g")
   var array = new Array[Int](numOfNodes.toInt+1)
   array(0)=0 
   val sc = new SparkContext(conf)
  val initialNode=scala.util.Random.nextInt(numOfNodes.toInt).toLong+1L
  val graph=createLouvainGraphFromMap("/home/honghuang/Documents/benchmark/100000/network.dat",sc,numOfNodes,initialNode)
  
  val edges=graph.edges.flatMap(f=>{
    Array((f.srcId,Array(f.dstId.toInt)))
  }).reduceByKey((e1,e2)=>{e1++e2})
  //edges.reduceByKey((e1,e2)=>{e1++e2})
  
  
  val t=edges.collectAsMap
  
  t.foreach(f=>{
    println("node"+f._1)
  val list=f._2
   list.foreach(f=>println(f))
  })
  
  //initialize count array
  t.foreach(f=>{
    array(f._1.toInt)=f._2.size
  })
  
  selectedNode.add(initialNode.toInt)
  val initialNeighbour=t(initialNode)
  neighbours=neighbours++initialNeighbour
  println("initial neighbours")
  neighbours.foreach(f=>{println(f)})
  // since this nodes is selected, all his neighbors has one less "innocent node"
  initialNeighbour.foreach(f=>{
    array(f)=array(f)-1
  })
  //adjacent nodes of neighbors have one less "innocent node"
  initialNeighbour.foreach(f=>{
    val adjacent=t(f)
    adjacent.foreach(q=>{
      array(q)=array(q)-1
    })
  })
  //prepare to loop
  var c=1
  while(selectedNode.size<requiredSize)
  {
    c=c+1
    println("iteration"+c)
  var index=(0-1)
    
  var count=(0-1) 
  
  
  //after this loop, index contains the next node to choose
  neighbours.foreach(f=>{
    if(array(f)>count)
    {
      index=f
    }
  })
  val nextNode=index
  selectedNode.add(nextNode)
  neighbours=neighbours-nextNode
  //println("next node"+nextNode)
  val adjacent=t(nextNode)
  adjacent.foreach(f=>{
    if(!neighbours.contains(f)&&(!selectedNode.contains(f)))
    {
      val list=t(f)
      list.foreach(i=>{
        array(i)=array(i)-1
      })
      neighbours.add(f)
    }
  })
  
  //println("neighbours after")
  //neighbours.foreach(f=>{
  //  println(f)
  //})
  //println("complete list")
  //for(a<- 1 to numOfNodes.toInt)
  //{
  //  println(a+" "+array(a))
  //}
  //println("selected node")
  //selectedNode.foreach(f=>{
  //  println(f)
  //})
  }
  println("selectedNode")
  selectedNode.foreach(f=>{
    println(f)
  })
}
def createLouvainGraphFromMap(path: String, sc: SparkContext, numOfNodes: Long,initialNode:Long): Graph[Int, Double] = {

    val textFile = sc.textFile(path)

    val edg = textFile.flatMap(e => {
      val str = new StringTokenizer(e, "\t");
      val id1 = str.nextToken().toLong
      val id2 = str.nextToken().toLong
      var w=0.0
      if(str.hasMoreTokens())
       w = str.nextToken().toDouble
       else{
         w=1.0
       }
      
     
        Array(((id1, id2), w),((id2, id1), w))
      
    })
    val edges = edg.distinct
    edges.collect().foreach(f => println(f))
    val edgeArray = edges.map { case (id, d) => (1, Edge(id._1, id._2, d)) }
    val v = edgeArray.values.collect
    v.foreach(f => println(f))
    val relationships: RDD[Edge[Double]] =
      sc.parallelize(v)

    val numNodes = numOfNodes
    var vertices = new Array[(Long, Int)](0)   //0 not selected 1 neighbor 2 selected
    var i = 0L
    while (i < numNodes) {
      if(i+1!=initialNode){
      vertices = vertices ++ Array((i + 1, 0))
      }else{
        vertices = vertices ++ Array((i + 1, 2))
      }
      i += 1
    }

    val nodes = sc.parallelize(vertices)
    val graph = Graph(nodes, relationships)
    graph.edges.collect.foreach(f => println(f))
    graph.vertices.collect.foreach(f => println(f))

    /*
     * collect adjacent weights of nodes in the graph
     * */
    //fill in adjacent weights with mapreduceTriplet
    val vertexGroup: VertexRDD[(Double)] = graph.mapReduceTriplets(et => Iterator((et.srcId, et.attr), (et.dstId, et.attr)), (e1, e2) => e1 + e2)

    

    //initializing the vertex. for the purpose of verification, the selfWeight variable is set to 1.0, which means the total weight of the internal edges of the community in the "previous level" is 0.5. Because this is an undirected graph, the self-loop is weighted 0.5x2=1.0
    graph

  }
}