package tt
import scala.collection.mutable.Set  
import scala.util.control.Breaks._
import scala.sys.process.Process
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
import org.apache.spark.graphx.lib.PageRank
object graphSample {
	def main(args: Array[String]): Unit = {
	  
	  println("real run");
    println("scala works")
    println("Spark initialization")
    /*
     * Spark initialization
     * */

    val conf = new SparkConf().setAppName("hello").setMaster("local").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g")
    //System.setProperty("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)

    println("test")
    val sample=uniformRandomNodeSelection("/home/honghuang/Synchronous-Louvain/benchmark/binary_network_5000/5000_0.65/network.dat",sc,5000,600)
    sample.edges.collect.foreach(f=>println("from"+f.srcId+" to "+f.dstId))
   
    
	}
	
	def uniformRandomNodeSelection(path:String,sc:SparkContext,numOfNodes:Long,sampleSize:Long): Graph[VertexInfo, Double]={
	 val graph=createLouvainGraphFromMap(path,sc,numOfNodes)
	  var set=Set.empty[Long]
	  while(set.size<sampleSize)
	  {
		  val r = scala.util.Random
		  val nodeIndex=r.nextInt(numOfNodes.toInt)+1
		  set+=nodeIndex
		  
	  }
	  val subgraph=graph.subgraph(vpred = (id, attr) => set.contains(id))
	  subgraph
	}
	def pageRankSample(path:String,sc:SparkContext,numOfNodes:Long): Graph[VertexInfo, Double]={
	  val graph=createLouvainGraphFromMap(path,sc,numOfNodes)
    val result=PageRank.run(graph,100,0.15)
    
    val vertices=result.vertices
    val id=vertices.id
    result.vertices.collect.foreach(f=>println(f))
    val ver=result.vertices  //vertex id and page rank values
   val total=ver.reduceByKey(_+_)
   println("total"+total)
	  return graph
	}
	def createLouvainGraphFromMap(path: String, sc: SparkContext, numOfNodes: Long): Graph[VertexInfo, Double] = {

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
      
      
        Array(((id1, id2), w))
      
    })
    val edges = edg
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

    if (Setting.checkModularityCalculation) {
      var LouvainGraph = graph.outerJoinVertices(vertexGroup)((vid, name, weight) => {
        val Info = new VertexInfo(); Info.selfWeight = 0.0; if (vid < Setting.treshold) { Info.community = 1 } else { Info.community = Setting.treshold; }
        Info.communitySigmaTot = weight.getOrElse(0.0);
        Info.adjacentWeight = weight.getOrElse(0.0);
        Info
      })
      Logger.writeLog("adjacentWeights")

      LouvainGraph.vertices.collect().foreach(f => Logger.writeLog(f.toString))
      Logger.writeLog("Louvain graph edges")
      LouvainGraph.edges.collect.foreach(f => Logger.writeLog(f.toString))
      return LouvainGraph
    }

    //initializing the vertex. for the purpose of verification, the selfWeight variable is set to 1.0, which means the total weight of the internal edges of the community in the "previous level" is 0.5. Because this is an undirected graph, the self-loop is weighted 0.5x2=1.0
    var LouvainGraph = graph.outerJoinVertices(vertexGroup)((vid, name, weight) => { val Info = new VertexInfo(); Info.selfWeight = 0.0; Info.community = vid; Info.communitySigmaTot = weight.getOrElse(0.0); Info.adjacentWeight = weight.getOrElse(0.0); Info })

    Logger.writeLog("adjacentWeights")

    LouvainGraph.vertices.collect().foreach(f => Logger.writeLog(f.toString))
    Logger.writeLog("Louvain graph edges")
    LouvainGraph.edges.collect.foreach(f => Logger.writeLog(f.toString))
    LouvainGraph

  }
}