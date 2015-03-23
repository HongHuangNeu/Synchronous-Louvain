package sample
import scala.util.control.Breaks._
import scala.collection.mutable.Set
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
object forestFire {
def main(args: Array[String]): Unit = {
  var selectedNode=Set.empty[Long]
  var neighbours=Set.empty[Long]
  val requiredSize=1000
 val numOfNodes=100000L
   val conf = new SparkConf().setAppName("hello").setMaster("local").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g")
    //System.setProperty("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)
  val initialNode=scala.util.Random.nextInt(numOfNodes.toInt).toLong+1L
  val graph=createLouvainGraphFromMap("/home/honghuang/Documents/benchmark/100000/network.dat",sc,numOfNodes,initialNode)
  
  val edges=graph.edges.flatMap(f=>{
    Array((f.srcId,Array(f.dstId)))
  }).reduceByKey((e1,e2)=>{e1++e2})
  //edges.reduceByKey((e1,e2)=>{e1++e2})
  
  
  val adjacent=edges.collectAsMap
  
  adjacent.foreach(f=>{
    println("node"+f._1)
  val list=f._2
   list.foreach(f=>println(f))
  })
  selectedNode.add(initialNode)
  
  var frontier=Set.empty[Long]
  var burnSet=Set.empty[Long]
  frontier.add(initialNode)
  //burnSet=burnSet++adjacent(initialNode)
  println(geoRandom)
   println(geoRandom)
    println(geoRandom)
    
    breakable {
    while(selectedNode.size<requiredSize)
    {
    //if the fire dies out, randomly select a new node to restart
      if(frontier.isEmpty)
      {
        val remainSet=((1L to numOfNodes).toSet--selectedNode).toVector
        val nextNode=remainSet(scala.util.Random.nextInt(remainSet.size))
        selectedNode.add(nextNode.toLong)
        frontier.add(nextNode)
        println("the next node to start"+nextNode)
        if(selectedNode.size>=requiredSize)
        {println("big enough!")
          break
          }
      }
      frontier.foreach(
      f=>{
        println(f+"is being considered")
        val burnlist=addNewNode(f,adjacent,selectedNode)
        burnSet=burnSet++burnlist
        selectedNode=selectedNode++burnSet
      }    
      )
      
      frontier=burnSet
      println("the following nodes are being burned")
      burnSet.foreach(f=>println(f))
      burnSet=Set.empty[Long]
    		  
    }
}
  println("selected nodes")
      selectedNode.foreach(f=>println(f))
      val subgraph=graph.subgraph(vpred = (id, attr) => selectedNode.contains(id))
      
      val sw = new FileWriter("/home/honghuang/Documents/benchmark/100000/sample2.dat")
  subgraph.edges.flatMap(f=>Array((f.srcId,f.dstId))).collectAsMap.foreach(
  f=>{
    sw.write(f._1+"\t"+f._2+"\n")
  }    
  )
      sw.flush()
sw.close()
  } 
  def addNewNode(currentNode:Long,adjacent:Map[Long,Array[Long]],selectedNode:Set[Long]):Set[Long]={
   
    var result=Set.empty[Long]
    val neighbours=adjacent(currentNode)
    val remain=neighbours.toSet--selectedNode
    val r=scala.util.Random.shuffle(remain.toList)
    var iterator=r.iterator
    
    var numOfElements=geoRandom
    println("the selected random number:"+numOfElements)
    var count=0
    while(iterator.hasNext&&count<numOfElements)
    {
      count=count+1
      result.add(iterator.next)
    }
    result
  }
  def geoRandom():Int={
    val rand=scala.util.Random.nextDouble
    val pBurn=0.2
    val p=1-pBurn
    return scala.math.floor(loge(rand) / loge(1-p)).toInt
  }
  def loge(x:Double)={
    scala.math.log(x)/scala.math.log(java.lang.Math.E)
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