package sample
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
object snowball {
def main(args: Array[String]): Unit = {
  var selectedNode=Set.empty[Long]
  var neighbours=Set.empty[Long]
  val requiredSize=1000
 val numOfNodes=100000L
   val conf = new SparkConf().setAppName("hello").setMaster("local").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g")
    //System.setProperty("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)
  val initialNode=4L
  val graph=createLouvainGraphFromMap("/home/honghuang/Documents/benchmark/100000/network.dat",sc,numOfNodes,initialNode)
  
  val edges=graph.edges.flatMap(f=>{
    Array((f.srcId,Array(f.dstId)))
  }).reduceByKey((e1,e2)=>{e1++e2})
  //edges.reduceByKey((e1,e2)=>{e1++e2})
  
  
  val t=edges.collectAsMap
  
  t.foreach(f=>{
    println("node"+f._1)
  val list=f._2
   list.foreach(f=>println(f))
  })
  selectedNode.add(initialNode)
  val initialNeighbour=edges.collectAsMap()(initialNode)
  neighbours=neighbours++initialNeighbour
  println("initialNeighbours")
neighbours.foreach(f=>printf(""+f))
while(selectedNode.size<requiredSize){
var next=getNextNode(selectedNode, neighbours,t)
if(next<0)
{
  next=getNextInitial(selectedNode,t)
}
println("nextNode selected"+next)
 selectedNode.add(next)
 val adjacentsOfNextNode=t(next).toSet
 
 neighbours=neighbours++adjacentsOfNextNode--selectedNode
 println("the neighbours after adding"+next)
 neighbours.foreach(f=>println(f))
}
  println("selected nodes")
  selectedNode.foreach(f=>println(f))
  val subgraph=graph.subgraph(vpred = (id, attr) => selectedNode.contains(id))
  val testSet=Set.empty[Long]
  subgraph.edges.flatMap(f=>Array((f.srcId,f.dstId))).collectAsMap.foreach(f=>{testSet.add(f._1)
    testSet.add(f._2)
    })
    println("testSet size"+testSet.size)
    val sw = new FileWriter("/home/honghuang/Documents/benchmark/100000/sample.dat")
  subgraph.edges.flatMap(f=>Array((f.srcId,f.dstId))).collectAsMap.foreach(
  f=>{
    sw.write(f._1+"\t"+f._2+"\n")
  }    
  )
}

def getNextInitial(selectedNode:Set[Long],list:Map[Long,Array[Long]]):Long={
  val set=list.keySet
  val effectiveSet=set--selectedNode
  effectiveSet.toList(scala.util.Random.nextInt(effectiveSet.size))
}

def getNextNode(selectedNode:Set[Long], neighbours:Set[Long],list:Map[Long,Array[Long]]):Long={
  var countMap:Map[Long,Long]= Map.empty[Long,Long]
  var i=0L
  if(neighbours.size==0)
  {return -1L}
  neighbours.foreach(f=>{
    i=f
    var adjacent=list(f)
    var count=0L
    adjacent.foreach(f=>{
      if((!selectedNode.contains(f))&&(!neighbours.contains(f)))
      {
        count=count+1L
      }
    })
    countMap=countMap+(f->count)
  })
  
  countMap.foreach(f=>{
  println("node"+f._1+" has"+f._2+"neighbours")})
 
  var index=i
  var count=countMap(index)
  
  println("chosen node"+index+"count"+count)
  countMap.foreach(f=>{
    if(f._2>count)
      {count=f._2
      index=f._1}
  })
  index
}


def getNextInitial(numOfElements:Long,set:Set[Long]):Long={
  val remainList=getSubset(numOfElements,set)
  val list=remainList.toArray
  val size=list.size
  val index=scala.util.Random.nextInt(size)
  list(index)
}

def getSubset(numOfElements:Long,set:Set[Long]):Set[Long]={
  var allNode=Set.empty[Long]
   for( a <- 1L to numOfElements){
         //println( "Value of a: " + a );
         allNode.add(a)
         
}
  val result=allNode--set
  result
}
def addNode(graph:Graph[Int, Double],node:Long):Graph[Int, Double]={
  val tmp=graph.mapVertices((id,nodeType)=>{
  if(id==node)
  {
    2
  }
  else{
    nodeType
  }
  }  
  )
  tmp
}
def findNextNode(graph:Graph[Int, Double]):Long={
  val vertexGroup: VertexRDD[Array[Int]] = graph.mapReduceTriplets(et => Iterator((et.srcId, Array(et.dstAttr)), (et.dstId, Array(et.srcAttr))), (e1, e2) => e1 ++ e2)
  val newGroup = graph.outerJoinVertices(vertexGroup)((vid, nodeType, neighbours) => {
    if(nodeType==1)
    {
      var count=0
      neighbours.getOrElse(Array(100)).foreach(f=>{
        if(f==0)
          count=count+1
      })
      count
    }else{
      -1
    }
    
  }).vertices.filter(f=>{f._2>=0})
  if(newGroup.count==0)
  {return -1L}
  
  val map=newGroup.collectAsMap
 
  var (index,count)=map.head
  map.foreach(f=>{
    if(f._2>count){
      count=f._2
      index=f._1
    }
  })
  index
}

def findNeighbour(graph:Graph[Int, Double]): Graph[Int, Double]={
  val vertexGroup: VertexRDD[Array[Int]] = graph.mapReduceTriplets(et => Iterator((et.srcId, Array(et.dstAttr)), (et.dstId, Array(et.srcAttr))), (e1, e2) => e1 ++ e2)

  val newGraph = graph.outerJoinVertices(vertexGroup)((vid, nodeType, neighbours) => {
    if(nodeType==2)
    {  2}
      else if(nodeType==1)
        {1}
        else{
          var isNeighbour=false
          neighbours.getOrElse(Array(100)).foreach(f=>{
            if(f==2)
              isNeighbour=true
          })
          
          if(isNeighbour)
          {1}else{
            0
          }
        }
  })
  
  newGraph
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