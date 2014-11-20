package tt

class VertexInfo extends Serializable{
	var community = -1L 
	var communitySigmaTot = 0.0 //this value might be outdated in the execution
	var selfWeight = 0.0  // weight of self loop, used when the node represents an aggregation of internal nodes of the same community in the previous level 
	var adjacentWeight = 0.0;  //outgoing edges except the self loop
	var changed = false 
	
	override def toString(): String = { 
    "{community:"+community+",communitySigmaTot:"+communitySigmaTot+ 
     ",selfWeight:"+selfWeight+",adjacentWeight:"+adjacentWeight+"}" 
   } 


}