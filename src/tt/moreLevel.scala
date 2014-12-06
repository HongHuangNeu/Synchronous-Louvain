package tt

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
import java.util.StringTokenizer;
import java.io.IOException
object moreLevel {
  /*
   * Graph Initialization
   * */

  def createLouvainGraphFromMap(path: String, sc: SparkContext, numOfNodes: Long): Graph[VertexInfo, Double] = {

    val textFile = sc.textFile(path)

    val edg = textFile.flatMap(e => {
      val str = new StringTokenizer(e, "\t");
      val id1 = str.nextToken().toLong
      val id2 = str.nextToken().toLong
      val w = str.nextToken().toDouble
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

  def louvainOneLevel(initialGraph: Graph[VertexInfo, Double], sc: SparkContext, probability: Double): Graph[VertexInfo, Double] = {
    var changed = false
    var counter = 0
    var converge = false
    var LouvainGraph = initialGraph

    var gw = 0.0
    /*
		     * calculate total weight of the network
		     * */
    //The total weight of the network, it is twice the actual total weight of the whole graph.Because the self-loop will be considered once, the other edges will be considered twice.
    val graphWeight = LouvainGraph.vertices.values.map(v => v.selfWeight + v.adjacentWeight).reduce(_ + _)
    Logger.writeLog("total weight of the graph:" + graphWeight)
    var totalGraphWeight = sc.broadcast(graphWeight)
    gw = graphWeight
    Logger.writeLog("initial modularity" + moreLevel.modularity(initialGraph, graphWeight))
    Logger.writeAdditionalLog("initial modularity" + moreLevel.modularity(initialGraph, graphWeight))
    if (Setting.checkModularityCalculation) {
      Logger.terminates
      sys.exit(0)
    }
    do {

      /*
		     *operations of collecting sigmaTot 
		     * */
      //Calculate sigma tot for each community
      val sigmaTot = LouvainGraph.vertices.values.map(v => (v.community, v.selfWeight + v.adjacentWeight)).reduceByKey(_ + _)
      //collect the result as map for look up
      val sigmaTotMap = sigmaTot.collectAsMap();
      //   Logger.writeLog("The sigmaTot map" + sigmaTotMap)
      //assign to each vertex the sigmaTot value of its community

      val newVert = LouvainGraph.vertices.map { case (id, d) => { d.communitySigmaTot = sigmaTotMap(d.community); (id, d) } }
      val newLouvainGraph = Graph(newVert, LouvainGraph.edges)

      //   newLouvainGraph.vertices.collect().foreach(f => Logger.writeLog("vertice print" + f))
      /*
		     * exchange community information and sigmaTot
		     * */
      //exchange community information and sigmaTot, prepare to calculate k_i_in
      val communityInfo = newLouvainGraph.mapReduceTriplets(exchangeMsg, mergeMsg) //The problem is, when mapReduceTriplet, only work on Louvain graph, not newLouvain Graph.
      //println("sigmaTot knowledge of neighbours")
      //communityInfo.values.collect.foreach(f=>println(f))

      //  communityInfo.values.collect().foreach(f => Logger.writeLog("neighbouring info" + f))
      /*
		     * update community
		     * */
      val newCom = newLouvainGraph.outerJoinVertices(communityInfo)((vid, v, d) => {
        var maxGain = 0.0
        val bigMap = d.reduceLeft(_ ++ _);
        if (bigMap.contains(v.community)) { maxGain = q(v.community, v.community, v.communitySigmaTot, bigMap(v.community)._2, v.adjacentWeight, v.selfWeight, graphWeight) } // fixed 2
        else {
          maxGain = 0.0 /*if bigMap does not contain the community of this node, the only
		        reason is that he is in the community with only himself, in this case, removing the node from the current community makes no difference to the total modularity, because you are doing nothing*/
        }
        var bestCommunity = v.community
        //  Logger.writeLog("for node " + vid + " the gain of staying in" + bestCommunity + " is" + maxGain + "the sigmaTot of the current community is" + v.communitySigmaTot)
        bigMap.foreach {
          case (communityId, (sigmaTot, edgeWeight)) => {
            val gain = q(v.community, communityId, sigmaTot, edgeWeight, v.adjacentWeight, v.selfWeight, graphWeight) //fixed 2
            //     Logger.writeLog("for node" + vid + " the gain of moving to community " + communityId + " is " + gain + " " + "the communitySigmaTot is" + sigmaTot)
            if (gain > maxGain) {
              maxGain = gain
              bestCommunity = communityId
            }
          }

        };
        val r = scala.util.Random
        if (v.community == bestCommunity) {
          v.converge = true
        } else {
          v.converge = false
        }
        if (v.community != bestCommunity && r.nextFloat <= probability) {
          v.community = bestCommunity
          v.changed = true
        } else {
          v.changed = false
        }

        v
      })

      // val conv = newCom.vertices.values.map(v => v.converge).reduce(_ && _)// may be problematic because of lazy evaluation

      //if strnge things happen, remove this line
      newLouvainGraph.vertices.unpersist(blocking = false)
      newLouvainGraph.edges.unpersist(blocking = false)
      communityInfo.unpersist(blocking = false)
      //if strnge things happen, remove this line ends

      val conv = newCom.vertices.values.map(v => if (v.converge) { 0 } else { 1 }).reduce(_ + _)
      if (conv == 0) { converge = true }
      else {
        converge = false
      }
      counter = counter + 1
      Logger.writeLog("run " + counter + "rounds")
      Logger.writeLog("changed?" + changed)
      //be careful
      val tmpGraph = LouvainGraph
      tmpGraph.vertices.unpersist(blocking = false)
      tmpGraph.edges.unpersist(blocking = false)
      //end becareful

      LouvainGraph = newCom
      //  Logger.writeLog("new vertives")
      //  LouvainGraph.vertices.collect().foreach(f => Logger.writeLog(f.toString))
      if (Setting.oneIteration) {
        Logger.writeLog("one iteration ends")
        Logger.terminates
        sys.exit(0)
      }
    } while (!converge)
    Logger.writeLog("execution ends")

    LouvainGraph.vertices.collect().foreach(f => Logger.writeLog(f.toString))
    Logger.writeLog("total runs" + counter)
    Logger.writeAdditionalLog("total runs" + counter) //to be removed
    val someSame = LouvainGraph.triplets.map(v => if (v.srcAttr.community == v.dstAttr.community) { 1 } else { 0 }).reduce(_ + _)
    Logger.writeLog("someSame " + someSame)

    val modul = modularity(LouvainGraph, gw)

    Logger.writeLog("final modularity" + modul)
    Logger.writeAdditionalLog("final modularity" + modul)
    LouvainGraph
  }
  /*
  * calculating modularity, question: is it necessary to multiply 2?
  * */
  def needMoreLevel(Graph: Graph[VertexInfo, Double]): Boolean = {
    val someSame = Graph.triplets.map(v => if (v.srcAttr.community == v.dstAttr.community) { 1 } else { 0 }).reduce(_ + _)
    if (someSame == 0) {
      Logger.writeLog("terminates!")
      return false;
    } else return true;
  }
  def modularity(Graph: Graph[VertexInfo, Double], graphWeight: Double): Double = {

    val m = graphWeight / 2
    //Calculate sigma tot for each community
    val sigmaTot = Graph.vertices.values.map(v => (v.community, v.selfWeight + v.adjacentWeight)).reduceByKey(_ + _)
    //collect the result as map for look up
    val sigmaTotMap = sigmaTot.collectAsMap();

    //calculate sum of weights of edges in the community
    val a = Graph.triplets.flatMap(e => {
      var result: Array[(Long, Double)] = Array()
      if (e.srcAttr.community == e.dstAttr.community) {
        result = Array((e.srcAttr.community, e.attr))
      } else {
        //do nothing
      }
      result
    }).reduceByKey(_ + _) //if every community consist of only the node itself, there will be no member in this map

    val internelEdgeSum = a.collectAsMap

    //calculate the self loops in the community
    val b = Graph.vertices.values.flatMap(e => {
      Array((e.community, e.selfWeight / 2)) // selftWeight is already doubled, so no need to double here
    }).reduceByKey(_ + _)

    val communityMap = b.collectAsMap

    var sum = 0.0

    communityMap.foreach(f => {

      var clusterWeight = 0.0
      if (internelEdgeSum.contains(f._1)) {
        clusterWeight = f._2 + internelEdgeSum(f._1)
      } else {
        clusterWeight = f._2 // for communities that contains only one node
      }

      val sigmaTot = sigmaTotMap(f._1)
      sum = sum + (clusterWeight / m - (sigmaTot / graphWeight) * (sigmaTot / graphWeight))
    })

    return sum
  }

  /*
  * compress the graph and proceed to the next level, community ids are maintained as node id for the next level
  * */
  def compressGraph(initialGraph: Graph[VertexInfo, Double], sc: SparkContext): Graph[VertexInfo, Double] = {
    //calculate internal weight within community
    val a = initialGraph.triplets.flatMap(e => {
      var result: Array[(Long, Double)] = Array()
      if (e.srcAttr.community == e.dstAttr.community) {
        result = Array((e.srcAttr.community, e.attr * 2))
      } else {
        //do nothing
      }
      result
    }).reduceByKey(_ + _) //if every community consist of only the node itself, there will be no member in this map
    Logger.writeLog("internel weights")
    a.collect().foreach(f => Logger.writeLog(f.toString))
    val internelWeights = a.collectAsMap

    val b = initialGraph.vertices.values.flatMap(e => {
      Array((e.community, e.selfWeight)) // selftWeight is already doubled, so no need to double here
    }).reduceByKey(_ + _)
    Logger.writeLog("selfWeihts sum")
    b.collect.foreach(f => Logger.writeLog(f.toString))

    val selfLoops = scala.collection.mutable.HashMap[Long, Double]()
    val selfWeights = b.collectAsMap
    selfWeights.foreach(f => {

      if (internelWeights.contains(f._1)) { selfLoops(f._1) = f._2 + internelWeights(f._1) }
      else {
        selfLoops(f._1) = f._2
      }
    })

    Logger.writeLog("self loops of new nodes")
    selfLoops.foreach(f => Logger.writeLog(f.toString))
    var vertices = new Array[(Long, VertexInfo)](0)
    selfLoops.foreach(f => {
      var v = new VertexInfo()
      v.community = f._1

      v.selfWeight = f._2
      vertices = vertices ++ Array((f._1, v))
    })

    //calculate edge weights between nodes
    val edg = initialGraph.triplets.flatMap(e => {
      var r: Array[((Long, Long), Double)] = Array()
      if (e.srcAttr.community < e.dstAttr.community) {
        r = Array(((e.srcAttr.community, e.dstAttr.community), e.attr))
      } else if (e.srcAttr.community > e.dstAttr.community) {
        r = Array(((e.dstAttr.community, e.srcAttr.community), e.attr))
      }
      r
    }).reduceByKey(_ + _)

    var edges = new Array[Edge[Double]](0)

    val edgeMap = edg.collectAsMap
    Logger.writeLog("new edges")
    edgeMap.foreach(f => Logger.writeLog(f.toString))

    edgeMap.foreach(f => edges = edges ++ Array(Edge(f._1._1, f._1._2, f._2)))

    val relationships: RDD[Edge[Double]] =
      sc.parallelize(edges)
    val nodes = sc.parallelize(vertices)
    val graph = Graph(nodes, relationships)
    /*
     * collect adjacent weights of nodes in the graph
     * */
    //fill in adjacent weights with mapreduceTriplet
    val vertexGroup: VertexRDD[(Double)] = graph.mapReduceTriplets(et => Iterator((et.srcId, et.attr), (et.dstId, et.attr)), (e1, e2) => e1 + e2)

    //initializing the vertex. for the purpose of verification, the selfWeight variable is set to 1.0, which means the total weight of the internal edges of the community in the "previous level" is 0.5. Because this is an undirected graph, the self-loop is weighted 0.5x2=1.0
    var newGraph = graph.outerJoinVertices(vertexGroup)((vid, v, weight) => { v.communitySigmaTot = v.selfWeight + weight.getOrElse(0.0); v.adjacentWeight = weight.getOrElse(0.0); v })

    newGraph
  }
  private def exchangeMsg(et: EdgeTriplet[VertexInfo, Double]) = {
    val m1 = (et.dstId, Map(et.srcAttr.community -> (et.srcAttr.communitySigmaTot, et.attr)))
    val m2 = (et.srcId, Map(et.dstAttr.community -> (et.dstAttr.communitySigmaTot, et.attr)))
    Iterator(m1, m2)
  }
  private def mergeMsg(m1: Map[Long, (Double, Double)], m2: Map[Long, (Double, Double)]) = {
    val infoMap = scala.collection.mutable.HashMap[Long, (Double, Double)]()
    //Logger.writeLog("received message " + m1 + " and " + m2)
    m1.foreach(f => {
      if (infoMap.contains(f._1)) {
        val w = f._2._2
        val (sig, weight) = infoMap(f._1)
        infoMap(f._1) = (sig, weight + w)
      } else {
        infoMap(f._1) = f._2
      }

    })
    m2.foreach(f => {
      if (infoMap.contains(f._1)) {
        val w = f._2._2
        val (sig, weight) = infoMap(f._1)
        infoMap(f._1) = (sig, weight + w)
      } else {
        infoMap(f._1) = f._2
      }

    })
    infoMap.toMap
  }
  /*Louvain update:
   * Calculate the improvement like this: first remove the node from the current community i and become an isolated community by himself. Then try to add this node to one of the communities(neighboring communities or community i)
   * and fetch the maximum
   * */
  private def q(currCommunityId: Long, joinCommunityId: Long, joinCommunitySigmaTot: Double, edgeWeightInJoinCommunity: Double, adjacentWeight: Double, selfWeight: Double, totalEdgeWeight: Double): Double = {

    var joinOriginalCommunity = true
    if (currCommunityId == joinCommunityId) {
      joinOriginalCommunity = true
    } else {
      joinOriginalCommunity = false
    }
    val M = totalEdgeWeight;
    var k_i_in = 0.0

    k_i_in = edgeWeightInJoinCommunity;

    val k_i = adjacentWeight + selfWeight; //self-loop is included in the calculation of k_i 

    var sigma_tot = 0.0
    if (joinOriginalCommunity) { sigma_tot = joinCommunitySigmaTot - k_i; }
    /*if you are calculating gain of modularity for the current community,previously in the calculation
 		* of sigmaTot for the community the edge weight of the current node and his self loop(selfWeight) is 
 		* included. Now we are artificially "adding" the node the the original community and therefore the adjacent edges  
 		*self loop should not be included in the sigmaTot of the current community
 		* */

    else {
      sigma_tot = joinCommunitySigmaTot
    }

    var deltaQ = k_i_in - (k_i * sigma_tot / M)

    return deltaQ;
  }
}