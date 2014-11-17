
object test {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection._
import java.util.StringTokenizer;
val conf = new SparkConf().setAppName("hello").setMaster("local")
                                                  //> conf  : org.apache.spark.SparkConf = org.apache.spark.SparkConf@423798e2
val sc=new SparkContext(conf)                     //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| 14/11/17 16:25:38 WARN Utils: Your hostname, honghuang-VirtualBox resolves t
                                                  //| o a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface eth0)
                                                  //| 
                                                  //| 14/11/17 16:25:38 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to anot
                                                  //| her address
                                                  //| 14/11/17 16:25:38 INFO SecurityManager: Changing view acls to: honghuang,
                                                  //| 14/11/17 16:25:38 INFO SecurityManager: Changing modify acls to: honghuang,
                                                  //| 14/11/17 16:25:38 INFO SecurityManager: SecurityManager: authentication disa
                                                  //| bled; ui acls disabled; users with view permissions: Set(honghuang, ); users
                                                  //|  with modify permissions: Set(honghuang, )
                                                  //| 14/11/17 16:25:40 INFO Slf4jLogger: Slf4jLogger started
                                                  //| 14/11/17 16:25:40 INFO Remoting: Starting remoting
                                                  //| 14/11/17 16:25:41 INFO Remoting: Remoting started; listening on addresses :[
                                                  //| akka.tcp://sparkDriver@10.0.2.15:33765]
                                                  //| 14/11/17 16:25:41 INFO Remoting: Rem
                                                  //| Output exceeds cutoff limit.
//sc.wholeTextFiles("/home/honghuang/t").values.flatMap(_.split("\n\n").grouped(4).map(_.mkString("\n")))

//another thing
    val v=sc.wholeTextFiles("/home/honghuang/t").values.flatMap(_.split("\n\n").map(_.mkString(" ")))
                                                  //> 14/11/17 16:25:47 INFO MemoryStore: ensureFreeSpace(199835) called with curM
                                                  //| em=0, maxMem=541883105
                                                  //| 14/11/17 16:25:47 INFO MemoryStore: Block broadcast_0 stored as values in me
                                                  //| mory (estimated size 195.2 KB, free 516.6 MB)
                                                  //| v  : org.apache.spark.rdd.RDD[String] = FlatMappedRDD[2] at flatMap at test.
                                                  //| scala:15
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
                   list})                         //> matchactor  : org.apache.spark.rdd.RDD[(String, String)] = FlatMappedRDD[3]
                                                  //|  at flatMap at test.scala:18
      val r=matchactor.join(matchactor).map{case (k,v)=>(v._1,v._2)}
                                                  //> 14/11/17 16:25:47 INFO FileInputFormat: Total input paths to process : 1
                                                  //| 14/11/17 16:25:48 INFO FileInputFormat: Total input paths to process : 1
                                                  //| 14/11/17 16:25:48 INFO CombineFileInputFormat: DEBUG: Terminated node alloc
                                                  //| ation with : CompletedNodes: 1, size left: 0
                                                  //| r  : org.apache.spark.rdd.RDD[(String, String)] = MappedRDD[7] at map at te
                                                  //| st.scala:40
      
    
    matchactor.collect().foreach(f=>println(f))   //> 14/11/17 16:25:48 INFO SparkContext: Starting job: collect at test.scala:43
                                                  //| 
                                                  //| 14/11/17 16:25:48 INFO DAGScheduler: Got job 0 (collect at test.scala:43) w
                                                  //| ith 1 output partitions (allowLocal=false)
                                                  //| 14/11/17 16:25:48 INFO DAGScheduler: Final stage: Stage 0(collect at test.s
                                                  //| cala:43)
                                                  //| 14/11/17 16:25:48 INFO DAGScheduler: Parents of final stage: List()
                                                  //| 14/11/17 16:25:48 INFO DAGScheduler: Missing parents: List()
                                                  //| 14/11/17 16:25:48 INFO DAGScheduler: Submitting Stage 0 (FlatMappedRDD[3] a
                                                  //| t flatMap at test.scala:18), which has no missing parents
                                                  //| 14/11/17 16:25:48 INFO MemoryStore: ensureFreeSpace(2560) called with curMe
                                                  //| m=199835, maxMem=541883105
                                                  //| 14/11/17 16:25:48 INFO MemoryStore: Block broadcast_1 stored as values in m
                                                  //| emory (estimated size 2.5 KB, free 516.6 MB)
                                                  //| 14/11/17 16:25:48 INFO DAGScheduler: Submitting 1 missing tasks from Stage 
                                                  //| 0 (FlatMappedRDD[3] at flatMap at test.scala:18)
                                                  //| 14/11/17 16:25:48 INFO
                                                  //| Output exceeds cutoff limit.
    r.filter{case (k,v)=>k!=v}.foreach(f=>println(f))
                                                  //> 14/11/17 16:25:49 INFO SparkContext: Starting job: foreach at test.scala:44
                                                  //| 
                                                  //| 14/11/17 16:25:49 INFO DAGScheduler: Registering RDD 3 (flatMap at test.sca
                                                  //| la:18)
                                                  //| 14/11/17 16:25:49 INFO DAGScheduler: Registering RDD 3 (flatMap at test.sca
                                                  //| la:18)
                                                  //| 14/11/17 16:25:49 INFO DAGScheduler: Got job 1 (foreach at test.scala:44) w
                                                  //| ith 1 output partitions (allowLocal=false)
                                                  //| 14/11/17 16:25:49 INFO DAGScheduler: Final stage: Stage 1(foreach at test.s
                                                  //| cala:44)
                                                  //| 14/11/17 16:25:49 INFO DAGScheduler: Parents of final stage: List(Stage 2, 
                                                  //| Stage 3)
                                                  //| 14/11/17 16:25:49 INFO DAGScheduler: Missing parents: List(Stage 2, Stage 3
                                                  //| )
                                                  //| 14/11/17 16:25:49 INFO DAGScheduler: Submitting Stage 2 (FlatMappedRDD[3] a
                                                  //| t flatMap at test.scala:18), which has no missing parents
                                                  //| 14/11/17 16:25:49 INFO MemoryStore: ensureFreeSpace(2984) called with curMe
                                                  //| m=202395, maxMem=541883105
                                                  //| 14/11/17 16:25:49 INFO MemoryStore: Block broadcast_2 stor
                                                  //| Output exceeds cutoff limit./
}