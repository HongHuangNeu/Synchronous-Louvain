
object test {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(58); 
  println("Welcome to the Scala worksheet")
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection._
import java.util.StringTokenizer;$skip(237); ;
val conf = new SparkConf().setAppName("hello").setMaster("local");System.out.println("""conf  : org.apache.spark.SparkConf = """ + $show(conf ));$skip(30); 
val sc=new SparkContext(conf);System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(225); 
//sc.wholeTextFiles("/home/honghuang/t").values.flatMap(_.split("\n\n").grouped(4).map(_.mkString("\n")))

//another thing
    val v=sc.wholeTextFiles("/home/honghuang/t").values.flatMap(_.split("\n\n").map(_.mkString(" ")));System.out.println("""v  : org.apache.spark.rdd.RDD[String] = """ + $show(v ));$skip(821); 
    // v.foreach(f=>println(f+"\n\n\n\n"))
    
    val matchactor=v.flatMap(f=>{
      val s=new  StringTokenizer(f,"\t\t");
      var list=List(("",""))
      var actorName="";
	               if(s.hasMoreTokens())
	               {
	            	    actorName=s.nextToken()
	            	  
	               }
	               while(s.hasMoreTokens())
	               {
	            	   val l=s.nextToken();
	            	   val t=new  StringTokenizer(l,"(");
	            	   if(t.hasMoreTokens())
	            	   {val str=t.nextToken().replaceAll("\"", "").trim()
	            	     val name=actorName.trim().equals("")
	            	     if(!str.equals("")&&(!name.equals("")))
	            		list=list++List((str,actorName.trim()))
	            	     
	            	   }
	               }
                   list});System.out.println("""matchactor  : org.apache.spark.rdd.RDD[(String, String)] = """ + $show(matchactor ));$skip(69); 
      val r=matchactor.join(matchactor).map{case (k,v)=>(v._1,v._2)};System.out.println("""r  : org.apache.spark.rdd.RDD[(String, String)] = """ + $show(r ));$skip(60); 
      
    
    matchactor.collect().foreach(f=>println(f));$skip(54); 
    r.filter{case (k,v)=>k!=v}.foreach(f=>println(f))}

/*
    
    val v=sc.wholeTextFiles("/home/honghuang/mat2").values.flatMap(_.split("\n\n").map(_.mkString(" ")))
     
    val matchactor=v.flatMap(f=>{
      val s=new  StringTokenizer(f,"\t\t");
      var list=List(("",""))
      var actorName="";
	               if(s.hasMoreTokens())
	               {
	            	    actorName=s.nextToken()
	            	  
	               }
	               while(s.hasMoreTokens())
	               {
	            	   val l=s.nextToken();
	            	   val t=new  StringTokenizer(l,"(");
	            	   if(t.hasMoreTokens())
	            	   {val str=t.nextToken().replaceAll("\"", "").trim()
	            	     val name=actorName.trim().equals("")
	            	     if(!str.equals("")&&(!name.equals("")))
	            		list=list++List((str,actorName.trim()))
	            	     
	            	   }
	               }
                   list})
                   matchactor.collect().take(10).foreach(f=>println(f))
                   
     val r=matchactor.join(matchactor).map{case (k,v)=>(v._1,v._2)}
    
    // matchactor.collect().foreach(f=>println(f))
   
    r.filter{case (k,v)=>k!=v}.take(20).foreach(f=>println(f))
    val next = r.filter{case (k,v)=>k!=v}
    next.take(20).foreach(f=>println(f))
    
    /*for each entry, set distance to infinity unless*/
    val init=next.map{case((k,v))=>if(k.contains("Bacon")){(k,0)}else{(k,Integer.MAX_VALUE)}}
    
    println(init.count)
    val temp=next.filter{case (k,v)=>v.contains("B a c o n ,   K e v i n")||k.contains("B a c o n ,   K e v i n")}
    temp.collect().foreach(f=>println(f))
   
    /*find the actors of 1 distance away*/
    val once=temp.join(next).map{case (k,v)=>(v._1,v._2)}.filter{case (k,v)=>v.contains("B a c o n ,   K e v i n")||k.contains("B a c o n ,   K e v i n")}
    println("one distance away")
    once.distinct().take(10).foreach(f=>println(f))
    val oneList=once.map{case(k,v)=>((k,v),1)}.distinct()
    oneList.collect().foreach(f=>println(f))
    
    /*find the actors that are 2 distance */
    val two=once.join(next).map{case (k,v)=>(v._1,v._2)}.filter{case (k,v)=>v.contains("B a c o n ,   K e v i n")||k.contains("B a c o n ,   K e v i n")}
    two.distinct().take(10).foreach(f=>println(f))
    val twoList=two.map{case(k,v)=>((k,v),2)}
    twoList.collect().foreach(f=>println(f))
    
    
    /*find the actors that are 3 distance */
    val three=two.join(next).map{case (k,v)=>(v._1,v._2)}.filter{case (k,v)=>v.contains("B a c o n ,   K e v i n")||k.contains("B a c o n ,   K e v i n")}
    val threeList=three.map{case(k,v)=>((k,v),3)}
   threeList.collect().foreach(f=>println(f))
    //result.collect().foreach(f=>println(f))
    
    /*find the actors that are 4 distance */
    val four=three.join(next).map{case (k,v)=>(v._1,v._2)}.filter{case (k,v)=>v.contains("B a c o n ,   K e v i n")||k.contains("B a c o n ,   K e v i n")}
    val fourList=four.map{case(k,v)=>((k,v),4)}
    
    //result.collect().foreach(f=>println(f))
    
    /*find the actors that are 5 distance */
    val five=four.join(next).map{case (k,v)=>(v._1,v._2)}.filter{case (k,v)=>v.contains("B a c o n ,   K e v i n")||k.contains("B a c o n ,   K e v i n")}
    val fiveList=five.map{case(k,v)=>((k,v),5)}
    
    //result.collect().foreach(f=>println(f))
    /*find the actors that are 6 distance */
    val six=two.join(next).map{case (k,v)=>(v._1,v._2)}.filter{case (k,v)=>v.contains("B a c o n ,   K e v i n")||k.contains("B a c o n ,   K e v i n")}
    val sixList=six.map{case(k,v)=>((k,v),6)}
   
    //result.collect().foreach(f=>println(f))
   
    val sum=(oneList++twoList++threeList++fourList++fiveList++sixList).reduceByKey((v1,v2)=>Math.min(v1,v2))
    //val result=sum.map{case((k,v1),v)=>((k,v),List(v1))}.reduceByKey((t1,t2)=>t1++t2)
    		sum.collect().foreach(f=>println(f))
    		val result=sum.map{case((k,v1),v)=>((k,v),List(v1))}.reduceByKey((t1,t2)=>t1++t2)
    		result.collect().foreach(f=>println(f))
    		*
    		*/
}
