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
import scala.collection.mutable.Map
object MoteCarloCC {
  //node id->numbers of selected adjacent nodes
  private var currentNeighbours = collection.mutable.Map[Long, Int]().withDefaultValue(0)
def main(args: Array[String]): Unit = {
  val numOfNodes=8L
  val requiredSize=2L
  
  //var neighbours=Set.empty[Long]
  
  var vertexSet=Set.empty[Long]
 
  
  val maxIteration=5
  var currentSelected=randomlyInitialize(numOfNodes,requiredSize)
  println("initial selected")
  currentSelected.foreach(f=>{println(f)})
  var bestSelected=currentSelected
  var newSelected=Set.empty[Long]
  
  println("power"+MyPow.pow(2.0,3))
 
 (1L to numOfNodes).toList.foreach(f=>{
   vertexSet.add(f)
 })
 

   val conf = new SparkConf().setAppName("hello").setMaster("local").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g")
    //System.setProperty("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)
  val initialNode=4L//scala.util.Random.nextInt(numOfNodes.toInt).toLong+1L
  val graph=createLouvainGraphFromMap("/home/honghuang/exampleGraph",sc,numOfNodes,initialNode)
  
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
  initializeNeighbours(collection.mutable.Map() ++adjacent,currentSelected)
  println("initial neighbours")
  MoteCarloCC.currentNeighbours.keySet.foreach(f=>{println(f)})
  /*
  selectedNode.add(2L)
  selectedNode.add(1L)
  initializeNeighbours(collection.mutable.Map() ++adjacent,selectedNode)
  val newNeighbours=neighboursAfterRemoveAndAdd(collection.mutable.Map() ++adjacent,2L,4L,selectedNode)
  println("neighbours after")
  newNeighbours.foreach(f=>{println(f)})
  println()
  println("current neighbours")
  MoteCarloCC.currentNeighbours.foreach(f=>println(f))
return
  */
  val p=10*graph.edges.count/graph.vertices.count*log10(graph.vertices.count)
  
  
  
  var currentEQ=expansionQuality(collection.mutable.Map() ++adjacent,currentSelected)
  println("initial currentEQ"+currentEQ)
  var bestEQ=expansionQuality(collection.mutable.Map() ++adjacent,bestSelected)
  println("initial best EQ"+bestEQ)
  for( a <- 1 to maxIteration){
    println("iteration"+a)
    val v=randomNodeFromSet(currentSelected)
    println(" removed v"+v)
   var remain=vertexSet--currentSelected+v
    
   
    val w=randomNodeFromSet(remain)
    println("added w"+w)
   newSelected=currentSelected-v+w 
   val alpha=scala.util.Random.nextFloat
   val Q_new=expansionQuality(collection.mutable.Map() ++adjacent,v,w,currentSelected)//expansionQuality(collection.mutable.Map() ++adjacent,newSelected)
   println("currentEQ"+currentEQ)
   println("new EQ"+Q_new)
   val x=Q_new/currentEQ
   val y=p.toInt
   if(alpha<MyPow.pow(x,y))
   { println("probability trigered")
     MoteCarloCC.currentNeighbours=neighboursAfterRemoveAndAdd(collection.mutable.Map() ++adjacent,v,w,currentSelected) 
     currentSelected=newSelected
     currentEQ=Q_new
     println("currentEQ is now"+currentEQ)
   println("new current selected is now")
   currentSelected.foreach(f=>{println(f)})
   println("new current neighbours is now")
   MoteCarloCC.currentNeighbours.keySet.foreach(f=>{
     println(f)
   })
     if(currentEQ>bestEQ)
     {
       println("better than best!")
       bestSelected=currentSelected
       bestEQ=currentEQ
       println("best EQ is now"+bestEQ)
       println("best selected is now")
       bestSelected.foreach(f=>{println(f)})
     }
   }
  }

  val subgraph=graph.subgraph(vpred = (id, attr) => bestSelected.contains(id))
  
  val sw = new FileWriter("/home/honghuang/Documents/benchmark/100000/sample3.dat")
  subgraph.edges.flatMap(f=>Array((f.srcId,f.dstId))).collectAsMap.foreach(
  f=>{
    sw.write(f._1+"\t"+f._2+"\n")
  }    
  )
      sw.flush()
sw.close()
  
}

def neighboursAfterRemoveAndAdd(adjacent:Map[Long,Array[Long]],removeNode:Long,addNode:Long,selectedNode:Set[Long]):Map[Long,Int]={
  //be careful, the "selectedNode" variable here refer to the selected node set before removing v and adding w
  
  //When the node is remove, the "selected" count of the affected neighbours should decrease 1
  val setAfterRemove=selectedNode.clone()-removeNode
  var neighbours=MoteCarloCC.currentNeighbours.clone
  val removeArray=adjacent(removeNode)
  var selectedCount=0
  removeArray.foreach(f=>{
    if(neighbours.contains(f))
    {
      neighbours(f)=neighbours(f)-1
    }
    if(setAfterRemove.contains(f))
    {
      selectedCount=selectedCount+1
    }
  })
  
  //the node being removed may be a neighbour himself
  if(selectedCount!=0)
 neighbours(removeNode)=selectedCount
  
 //The node being added may previously be a neighbour himself
  neighbours.remove(addNode)
  
  val addArray=adjacent(addNode)
  addArray.foreach(f=>{
   if(!setAfterRemove.contains(f))
   {
    if(neighbours.contains(f))
    {
      neighbours(f)=neighbours(f)+1
    }else{
      neighbours(f)=1
    }
   }
  })
  
  neighbours=neighbours.filter(f=>{f._2!=0})
  neighbours
}  
  
def initializeNeighbours(adjacent:Map[Long,Array[Long]],selectedNode:Set[Long])={
  selectedNode.foreach(f=>{
    val array=adjacent(f)
    array.foreach(c=>{
      if(!selectedNode.contains(c))
      MoteCarloCC.currentNeighbours.update(c, MoteCarloCC.currentNeighbours(c)+1)
    })
  })
  println("initial neighbours")
  MoteCarloCC.currentNeighbours.foreach(f=>println(f))
  println()
  
}
  
def myPow(x:Double,y:Int):Double={
  var count=1.0
  for( a <- 1 to y){
    count=count*x
  }
  count
}
def log10(x:Double)={
    scala.math.log(x)/scala.math.log(10)
  }

def randomNodeFromSet(set:Set[Long]):Long={
  val rnd=scala.util.Random 
  set.toVector(rnd.nextInt(set.size))
}
def randomlyInitialize(numOfNodes:Long,sampleSize:Long):Set[Long]={
   var set=Set.empty[Long]
	 val result=scala.util.Random.shuffle((1L to numOfNodes).toList)
	 for( a <- 0L to sampleSize-1){
	   set.add(result(a.toInt))
	 }
   set
}

def expansionQuality(adjacent:Map[Long,Array[Long]],selectedNode:Set[Long]):Double={
  var neighbourSet=Set.empty[Long]
  selectedNode.foreach(f=>{
    neighbourSet=neighbourSet++adjacent(f)
  })
  neighbourSet=neighbourSet--selectedNode
  val sizeOfNeighbourSet=neighbourSet.size.toDouble
  val notSelected=collection.mutable.Set(adjacent.keySet.toSeq:_*)--selectedNode
  val sizeOfNotSelected=notSelected.size.toDouble
 // println("neighbours")
 // neighbourSet.foreach(f=>println(f))
 // println("not selected")
 // notSelected.foreach(f=>println(f))
  sizeOfNeighbourSet/sizeOfNotSelected
}

def expansionQuality(adjacent:Map[Long,Array[Long]],removeNode:Long,addNode:Long,selectedNode:Set[Long]):Double={
  val newNeighbours=neighboursAfterRemoveAndAdd(adjacent,removeNode,addNode,selectedNode)
  val notSelected=collection.mutable.Set(adjacent.keySet.toSeq:_*)--selectedNode
  newNeighbours.keySet.size.toDouble/notSelected.size.toDouble
  
}

def subgraph(adjacent:Map[Long,Array[Long]],selectedNode:Set[Long]):Map[Long,Array[Long]]={
  var t=adjacent
  adjacent.foreach(f=>{
    if(!selectedNode.contains(f._1))
    {
      t.remove(f._1)
    }
  })
  
  selectedNode.foreach(f=>{
    var neighbours=adjacent(f)
    val n=collection.mutable.Set(neighbours.toSet.toSeq:_*) //collection.mutable.Set(b.toSeq:_*)
    var notSelected=n--selectedNode
    var actualNeighbour=(n--notSelected).toArray
    t(f)=actualNeighbour
  })
  t
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