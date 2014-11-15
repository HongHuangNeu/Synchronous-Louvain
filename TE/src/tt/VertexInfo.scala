package tt

class VertexInfo extends Serializable{
	var community = -1L 
	var communitySigmaTot = 0.0 
	var selfWeight = 0.0  // self loop 
	var adjacentWeight = 0.0;  //out degree 
	var changed = false 
	
	override def toString(): String = { 
    "{community:"+community+",communitySigmaTot:"+communitySigmaTot+ 
     ",selfWeight:"+selfWeight+",adjacentWeight:"+adjacentWeight+"}" 
   } 


}