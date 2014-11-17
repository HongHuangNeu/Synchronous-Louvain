package tt
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import scala.collection._
import java.util.StringTokenizer;
case class Message(communityId:Long,communitySigmaTot:Double,edgeWeight:Double)
object test {

  def main(args: Array[String]): Unit = {
    
    println("scala works")
    println("Spark initialization")
    /*
     * Spark initialization
     * */
    val conf = new SparkConf().setAppName("hello").setMaster("local")
    val sc=new SparkContext(conf)
    
    /*
     * Graph initialization
     * */
    val nodes: RDD[(VertexId, String)] =
    sc.parallelize(Array((3L,
        "one"), (7L, "two"),
                       (5L, "three"), (2L, "four")))
                       // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
    sc.parallelize(Array(Edge(3L, 7L, 1.0),    Edge(5L, 3L, 2.0),
                       Edge(2L, 5L, 3.0), Edge(5L, 7L, 4.0)))
                       // Define a default user in case there are relationship with missing user	
    val defaultUser = ("Unknown")
    // Build the initial Graph
    val graph = Graph(nodes, relationships, defaultUser)
    graph.edges.collect().foreach(f=>println(f))
    graph.vertices.collect().foreach(f=>println(f))
    
    
    /*
     * collect adjacent weights of nodes in the graph
     * */
    //fill in adjacent weights with mapreduceTriplet
    val vertexGroup: VertexRDD[(Double)] =graph.mapReduceTriplets(et=>Iterator((et.srcId,et.attr), (et.dstId,et.attr)) , (e1,e2)=>e1+e2)
    val LouvainGraph=graph.outerJoinVertices(vertexGroup)((vid,name,weight)=>{val Info=new VertexInfo(); Info.community=vid;Info.communitySigmaTot=weight.getOrElse(0.0);Info.adjacentWeight=weight.getOrElse(0.0);Info  })
    println("adjacentWeights")
    LouvainGraph.vertices.collect().foreach(f=>println(f))
    
    /*
     * calculate total weight of the network
     * */
    //The total weight of the network
    val graphWeight = LouvainGraph.vertices.values.map(v=> v.selfWeight+v.adjacentWeight).reduce(_+_)
    println("total weight of the graph:"+graphWeight)
    var totalGraphWeight = sc.broadcast(graphWeight) 
    /*
     *operations of collecting sigmaTot 
     * */
    //Calculate sigma tot for each community
    val sigmaTot=LouvainGraph.vertices.values.map(v=>(v.community,v.selfWeight+v.adjacentWeight)).reduceByKey(_+_)
    //collect the result as map for look up
    val sigmaTotMap=sigmaTot.collectAsMap();
    println("The sigmaTot map"+sigmaTotMap)
    //assign to each vertex the sigmaTot value of its community
    val newLouvainGraph=LouvainGraph.mapVertices((id,d)=>{d.communitySigmaTot=sigmaTotMap(d.community); d})
    
    /*
     * exchange community information and sigmaTot
     * */
    //exchange community information and sigmaTot, prepare to calculate k_i_in
    val communityInfo =newLouvainGraph.mapReduceTriplets(exchangeMsg, mergeMsg)
    communityInfo.values.collect.foreach(f=>println(f))
    
    /*
     * update community
     * */
    val newCom=newLouvainGraph.outerJoinVertices(communityInfo)((vid,v,d)=>{
     var maxGain=0.0; 
     var bestCommunity=v.community
     val bigMap = d.reduceLeft(_ ++ _);
     bigMap.foreach{case (communityId,(sigmaTot,edgeWeight))=>{
       val gain=q(v.community,communityId, sigmaTot, edgeWeight, v.adjacentWeight, v.selfWeight, graphWeight)
      if(gain>maxGain)
      {
        maxGain=gain
        bestCommunity=communityId
      }
     }
     
     };
     v.community=bestCommunity
     v 
    })
    newCom.vertices.collect().foreach(f=>println(f))
  
    
    
    
    
    
    
    
    
  }
  private def exchangeMsg(et:EdgeTriplet[VertexInfo,Double]) = { 
    val m1 = (et.dstId,Map(et.srcAttr.community->(et.srcAttr.communitySigmaTot,et.attr))) 
    val m2 = (et.srcId,Map(et.dstAttr.community->(et.dstAttr.communitySigmaTot,et.attr))) 
    Iterator(m1, m2)     
    } 
  private def mergeMsg(m1:Map[Long,(Double,Double)],m2:Map[Long,(Double,Double)])={
    val infoMap = scala.collection.mutable.HashMap[Long,(Double,Double)]()
    m1.foreach(f=>{
      if(infoMap.contains(f._1))
      {
        val w=f._2._2
        val (sig,weight)=infoMap(f._1)
        infoMap(f._1)=(sig,weight+w)
      }else{
        infoMap(f._1)=f._2
      }
      
    })
    m2.foreach(f=>{
      if(infoMap.contains(f._1))
      {
        val w=f._2._2
        val (sig,weight)=infoMap(f._1)
        infoMap(f._1)=(sig,weight+w)
      }else{
        infoMap(f._1)=f._2
      }
      
    })
      infoMap.toMap
  }
  private def q(currCommunityId:Long, joinCommunityId:Long, joinCommunitySigmaTot:Double, edgeWeightInJoinCommunity:Double, adjacentWeight:Double, selfWeight:Double, totalEdgeWeight:Double) : Double = { 
	  	val isCurrentCommunity = (currCommunityId.equals(joinCommunityId)); 
 		val M = totalEdgeWeight;  
 	  	val k_i_in =   edgeWeightInJoinCommunity; 
 		 
 		val k_i = adjacentWeight + selfWeight; 
 		val sigma_tot = joinCommunitySigmaTot; 
 		 
 		var deltaQ =  0.0; 
 		if (!(isCurrentCommunity )) { 
 			deltaQ = k_i_in - ( k_i * sigma_tot / M) 
 			
 		} else{
 		  deltaQ=0.0
 		}
 		return deltaQ; 
   } 


}