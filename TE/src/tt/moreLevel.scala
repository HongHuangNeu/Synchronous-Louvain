package tt

import org.apache.spark.graphx.Graph
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
object moreLevel {
  /*
     * Graph initialization 
     * */
 def createLouvainGraph(path:String,sc:SparkContext,numOfNodes:Long) : Graph[VertexInfo,Double]={
  
   val src = scala.io.Source.fromFile(path) 
   //val src = scala.io.Source.fromFile("/home/honghuang/test.dat") 
    /*initialize nodes for the graph*/
    val numNodes=numOfNodes
    var vertices = new Array[(Long,Long)](0)
    var i=0L
    while(i<numNodes)
    {
      vertices=vertices++Array((i+1,i+1))
      i+=1
    }
    
    /*val nodes: RDD[(VertexId, Long)] =
    sc.parallelize(Array((3L,
        3L), (7L, 7L),
                       (5L, 5L), (2L, 2L)))*/
    val nodes=sc.parallelize(vertices)
                       
    // Create an RDD for edges
    val iter = src.getLines()  
    
    var edges=new Array[Edge[Double]](0)
    var added=new Array[(Long,Long)](0)
    while(iter.hasNext)
    {
      var line=iter.next()
      val str=new StringTokenizer(line,"\t")
      val x=str.nextToken().toLong
      val y=str.nextToken().toLong
      val weight=str.nextToken().toDouble
      if(!added.contains((x,y))&&(!added.contains((y,x))))
      {
        edges=edges++Array(Edge(x,y,weight))
        added=added++Array((x,y))
      }
    }
    /*val relationships: RDD[Edge[Double]] =
    sc.parallelize(Array(Edge(3L, 7L, 1.0),    Edge(5L, 3L, 2.0),
                       Edge(2L, 5L, 3.0), Edge(5L, 7L, 4.0)))*/
    val relationships: RDD[Edge[Double]] =
    sc.parallelize(edges)
                       // Define a default user in case there are relationship with missing user	
    val defaultUser = (1000000000L)
    // Build the initial Graph
    val graph = Graph(nodes, relationships, defaultUser)
    graph.edges.collect().foreach(f=>println(f))
    graph.vertices.collect().foreach(f=>println(f))
    
    
    /*
     * collect adjacent weights of nodes in the graph
     * */
    //fill in adjacent weights with mapreduceTriplet
    val vertexGroup: VertexRDD[(Double)] =graph.mapReduceTriplets(et=>Iterator((et.srcId,et.attr), (et.dstId,et.attr)) , (e1,e2)=>e1+e2)
    
    //initializing the vertex. for the purpose of verification, the selfWeight variable is set to 1.0, which means the total weight of the internal edges of the community in the "previous level" is 0.5. Because this is an undirected graph, the self-loop is weighted 0.5x2=1.0
    var LouvainGraph=graph.outerJoinVertices(vertexGroup)((vid,name,weight)=>{val Info=new VertexInfo(); Info.selfWeight=0.0;Info.community=vid;Info.communitySigmaTot=weight.getOrElse(0.0);Info.adjacentWeight=weight.getOrElse(0.0);Info  })

    println("adjacentWeights")
    LouvainGraph.vertices.collect().foreach(f=>println(f))
    println("Louvain graph edges")
    LouvainGraph.edges.collect.foreach(f=>println(f))
    LouvainGraph
 }
 
 def louvainOneLevel(initialGraph:Graph[VertexInfo,Double],sc:SparkContext):Graph[VertexInfo,Double]={
   var changed=false
    var counter=0
    var converge=false
    var LouvainGraph=initialGraph
   // val graphWeight = LouvainGraph.vertices.values.map(v=> v.selfWeight+v.adjacentWeight).reduce(_+_)
		//    println("total weight of the graph:"+graphWeight)
	//	    var totalGraphWeight = sc.broadcast(graphWeight) 
		//   val modularity=LouvainGraph.triplets.map(v=>if(v.srcAttr.community==v.dstAttr.community){v.attr-(v.srcAttr.adjacentWeight+v.srcAttr.selfWeight)*(v.dstAttr.adjacentWeight+v.dstAttr.selfWeight)/graphWeight}else{0.0}).reduce(_+_)*2/graphWeight
		  var gw=0.0
    do{  
		    /*
		     * calculate total weight of the network
		     * */
		    //The total weight of the network, it is twice the actual total weight of the whole graph.Because the self-loop will be considered once, the other edges will be considered twice.
		    val graphWeight = LouvainGraph.vertices.values.map(v=> v.selfWeight+v.adjacentWeight).reduce(_+_)
		    println("total weight of the graph:"+graphWeight)
		    var totalGraphWeight = sc.broadcast(graphWeight)
		    gw=graphWeight
		    /*
		     *operations of collecting sigmaTot 
		     * */
		    //Calculate sigma tot for each community
		    val sigmaTot=LouvainGraph.vertices.values.map(v=>(v.community,v.selfWeight+v.adjacentWeight)).reduceByKey(_+_)
		    //collect the result as map for look up
		    val sigmaTotMap=sigmaTot.collectAsMap();
		    println("The sigmaTot map"+sigmaTotMap)
		    //assign to each vertex the sigmaTot value of its community
		    
		    //val newLouvainGraph=LouvainGraph.mapVertices((id,d)=>{d.communitySigmaTot=sigmaTotMap(d.community); d})
		    val newVert = LouvainGraph.vertices.map { case (id, d) => {d.communitySigmaTot=sigmaTotMap(d.community);(id,d)} }
		    val newLouvainGraph = Graph(newVert, LouvainGraph.edges)
		    //graph edges
		    //println("graph edges")
		    //graph.edges.collect.foreach(f=>println(f))
		    newLouvainGraph.vertices.collect().foreach(f=>println("vertice print"+f))
		    /*
		     * exchange community information and sigmaTot
		     * */
		    //exchange community information and sigmaTot, prepare to calculate k_i_in
		    val communityInfo =newLouvainGraph.mapReduceTriplets(exchangeMsg, mergeMsg)//The problem is, when mapReduceTriplet, only work on Louvain graph, not newLouvain Graph.
		    //println("sigmaTot knowledge of neighbours")
		    //communityInfo.values.collect.foreach(f=>println(f))
		    
		    communityInfo.values.collect().foreach(f=>println("neighbouring info"+f))
		    /*
		     * update community
		     * */
		    val newCom=newLouvainGraph.outerJoinVertices(communityInfo)((vid,v,d)=>{
		      var maxGain=0.0
		      val bigMap = d.reduceLeft(_ ++ _);
		      if(bigMap.contains(v.community))
		      {maxGain=q(v.community,v.community,v.communitySigmaTot,bigMap(v.community)._2,v.adjacentWeight,v.selfWeight,graphWeight/2)}// note, here I divide the graphWeight by 2   //22
		      else
		      {
		        maxGain=0.0 /*if bigMap does not contain the community of this node, the only
		        reason is that he is in the community with only himself, in this case, removing the node from the current community makes no difference to the total modularity, because you are doing nothing*/
		      }
		      var bestCommunity=v.community
		     println("for node "+vid+" the gain of staying in"+bestCommunity+" is"+maxGain+"the sigmaTot of the current community is"+v.communitySigmaTot)
		     bigMap.foreach{case (communityId,(sigmaTot,edgeWeight))=>{
		       val gain=q(v.community,communityId, sigmaTot, edgeWeight, v.adjacentWeight, v.selfWeight, graphWeight/2)//22
		       println("for node"+vid+" the gain of moving to community "+communityId+" is "+gain+" "+"the communitySigmaTot is"+sigmaTot)
		      if(gain>maxGain)
		      {
		        maxGain=gain
		        bestCommunity=communityId
		      }
		     }
		     
		     };
		     val r = scala.util.Random
		     if(v.community==bestCommunity)
		     {
		       v.converge=true
		     }else{
		       v.converge=false
		     }
		     if(v.community!=bestCommunity&&r.nextFloat>=0.5)
		     {v.community=bestCommunity
		      v.changed=true 
		     }else{
		       v.changed=false
		     }
		     
		     v 
		    })
		    //newCom.vertices.collect().foreach(f=>println(f))
		  
		      converge=newCom.vertices.values.map(v=>v.converge).reduce(_&&_)
		    counter=counter+1
		    println("run "+counter+"rounds")
		    println("changed?"+changed)
		    
		    LouvainGraph=newCom
		    println("new vertives")
		    LouvainGraph.vertices.collect().foreach(f=>println(f))
    }while(!converge)
       println("execution ends")
       LouvainGraph.vertices.collect().foreach(f=>println(f))
       println("total runs"+counter)
      // println("start modularity "+modularity)
       val someSame=LouvainGraph.triplets.map(v=>if(v.srcAttr.community==v.dstAttr.community){1}else{0}).reduce(_+_)
       println("someSame "+someSame)
      // val temp=LouvainGraph.vertices.map(v=>(v._2.community,1)).reduceByKey((x,y)=>1)
      // println("different communities"+temp.count())
      // temp.collect().foreach(f=>println(f))
      val modularity=LouvainGraph.triplets.map(v=>if(v.srcAttr.community==v.dstAttr.community){v.attr-(v.srcAttr.adjacentWeight+v.srcAttr.selfWeight)*(v.dstAttr.adjacentWeight+v.dstAttr.selfWeight)/gw}else{0.0}).reduce(_+_)*2/gw
      println("modularity"+modularity)
    LouvainGraph
 }
 def compressGraph(initialGraph:Graph[VertexInfo,Double]):Graph[VertexInfo,Double]={
  
   val a=initialGraph.triplets.flatMap(e=>{
     var result:Array[(Long,Double)]=Array()
     if(e.srcAttr.community==e.dstAttr.community)
     {
       result=Array((e.srcAttr.community,e.attr))
     }else{
       
     }
     result
   })
   initialGraph
 }
 private def exchangeMsg(et:EdgeTriplet[VertexInfo,Double]) = { 
    val m1 = (et.dstId,Map(et.srcAttr.community->(et.srcAttr.communitySigmaTot,et.attr))) 
    val m2 = (et.srcId,Map(et.dstAttr.community->(et.dstAttr.communitySigmaTot,et.attr))) 
    Iterator(m1, m2)     
    } 
  private def mergeMsg(m1:Map[Long,(Double,Double)],m2:Map[Long,(Double,Double)])={
    val infoMap = scala.collection.mutable.HashMap[Long,(Double,Double)]()
    println("received message "+m1+" and "+m2)
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
  /*Louvain update:
   * Calculate the improvement like this: first remove the node from the current community i and become an isolated community by himself. Then try to add this node to one of the communities(neighboring communities or community i)
   * and fetch the maximum
   * */
  private def q(currCommunityId:Long, joinCommunityId:Long, joinCommunitySigmaTot:Double, edgeWeightInJoinCommunity:Double, adjacentWeight:Double, selfWeight:Double, totalEdgeWeight:Double) : Double = { 
	  	
	  	var joinOriginalCommunity=true
	  	if(currCommunityId==joinCommunityId)
	  	{
	  	  joinOriginalCommunity=true
	  	}else
	  	{
	  	  joinOriginalCommunity=false
	  	} 
 		val M = totalEdgeWeight;
 		var k_i_in=0.0
 		
 		
 	  	 k_i_in =   edgeWeightInJoinCommunity; 
 		
 		val k_i = adjacentWeight + selfWeight;//self-loop is included in the calculation of k_i 
 		
 		
 		var sigma_tot=0.0
 		if(joinOriginalCommunity)
 		{ sigma_tot = joinCommunitySigmaTot-k_i;}
 		/*if you are calculating gain of modularity for the current community,previously in the calculation
 		* of sigmaTot for the community the edge weight of the current node and his self loop(selfWeight) is 
 		* included. Now we are artificially "adding" the node the the original community and therefore the adjacent edges  
 		*self loop should not be included in the sigmaTot of the current community
 		* */
 		
 		else{
 		  sigma_tot=joinCommunitySigmaTot
 		}
 		
 		 
 		var deltaQ =   k_i_in - ( k_i * sigma_tot / M) 
 			
 		
 		return deltaQ; 
   } 
}