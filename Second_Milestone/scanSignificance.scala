package ir
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
object scanSignificance {
def main(args: Array[String]): Unit = {
  
  println("real run");
    println("scala works")
    println("Spark initialization")
    /*
     * Spark initialization
     * */

    val conf = new SparkConf().setAppName("hello").setMaster("local").set("spark.executor.memory", "3g")
    System.setProperty("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)	
   val significance=calculateSignificance(sc,"/home/honghuang/test_graph","/home/honghuang/test_community")
   println("significance"+significance)
}

/**
 * A small function to compute density of the graph
 * 
 */

 def calculateSignificance(sc:SparkContext,graphPath:String,communityPath:String):Double={
   val graph=createLouvainGraphFromMap(graphPath,sc,5)
    val vertices=graph.vertices
    val textFile = sc.textFile(communityPath)
    val community = textFile.flatMap(e => {
      val str = new StringTokenizer(e, "\t");
      val nodeId = str.nextToken().toLong
      val communityId = str.nextToken().toLong
      Array((nodeId,communityId))
    })
    val join=vertices.join(community)
    val nodes=join.map(v=>{v._2._1.community=v._2._2
    (v._1,v._2._1)  
    })
    val newGraph = Graph(nodes, graph.edges)
    
    val temp=newGraph.triplets.map{case e=>{
      if(e.dstAttr.community==e.srcAttr.community)
      {
        (e.dstAttr.community,1L)
      }else{
        (1L,-1L)
      }
      
    }}
    
    val tuples=temp.filter(e=>e._2>0L).reduceByKey(_+_)
    //tuples contains the community id and the corresponding number of internal edges
    tuples.foreach(f=>println(f+""))  
  var communityCount=community.map{case(id,communityId)=>(communityId,1)}
  communityCount=communityCount.reduceByKey(_+_)
 val joinInfo=tuples.join(communityCount)
 joinInfo.foreach(f=>println(f))
 
 var div=0.0
 var t=1.0
 
 val numNodes=5
 val numOfEdges=newGraph.edges.count
 println("edge count"+numOfEdges)
 val p=density(5,numOfEdges)   //total density
 joinInfo.collectAsMap.foreach(f=>{
   val numEdgeInC=f._2._1
   val numNodeInC=f._2._2
   println("numEdgeInC"+numEdgeInC)
   println("numNodeInC"+numNodeInC)
   val p_c=density(numNodeInC,numEdgeInC)
   val diverg=divergence(p_c,p)
   println("divergence"+diverg)
   println("pc"+p_c)
   println("c"+c(numNodeInC,2))
   div=c(numNodeInC,2)*diverg+div
   println("sum"+div)
   
 })
 
 println("final sum"+div)
 
 return div 
 }
 def density(numberNodes:Long,numberOfEdges:Long):Double={
   val p=(2*numberOfEdges.toDouble)/(numberNodes.toDouble*(numberNodes.toDouble-1))
   p
 }
 /*
  * Divergence
  * */
 def divergence(q:Double,p:Double):Double={
   if(q==1.0)
   {q*log2(q/p)}
   else{
   q*log2(q/p)+(1-q)*log2((1-q)/(1-p))
   }
 }
 
 def c(n_c:Long,up:Long):Long={
   
   n_c*(n_c-1)/((up)*(up-1))
 }
 def log2(x:Double)={
    scala.math.log(x)/scala.math.log(2)
  }
/*
   * Graph Initialization
   * */
  def createLouvainGraphFromMap(path: String, sc: SparkContext, numOfNodes: Long): Graph[tt.VertexInfo, Double] = {

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
      if (id1 > id2) { Array(((id2, id1), w)) }
      else {
        Array(((id1, id2), w))
      }
    })
    val edges = edg.distinct
    edges.collect().foreach(f => println(f))
    val edgeArray = edges.map { case (id, d) => (1, Edge(id._1, id._2, d)) }
    val v = edgeArray.values.collect
    v.foreach(f => println(f))
    val relationships: RDD[Edge[Double]] =
      sc.parallelize(v)

    val numNodes = numOfNodes
    var vertices = new Array[(Long, Long)](0)
    var i = 0L
    while (i < numNodes) {
      vertices = vertices ++ Array((i + 1, i + 1))
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
    var LouvainGraph = graph.outerJoinVertices(vertexGroup)((vid, name, weight) => { val Info = new tt.VertexInfo(); Info.selfWeight = 0.0; Info.community = vid; Info.communitySigmaTot = weight.getOrElse(0.0); Info.adjacentWeight = weight.getOrElse(0.0); Info })
    
    println("adjacentWeights")

    LouvainGraph.vertices.collect().foreach(f => println(f.toString))
    println("Louvain graph edges")
    LouvainGraph.edges.collect.foreach(f => println(f.toString))
    LouvainGraph
    
    }
}