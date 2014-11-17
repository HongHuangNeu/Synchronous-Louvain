
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
}
