
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
                                                  //| 14/11/20 19:33:59 WARN Utils: Your hostname, honghuang-VirtualBox resolves t
                                                  //| o a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface eth0)
                                                  //| 
                                                  //| 14/11/20 19:33:59 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to anot
                                                  //| her address
                                                  //| 14/11/20 19:33:59 INFO SecurityManager: Changing view acls to: honghuang,
                                                  //| 14/11/20 19:33:59 INFO SecurityManager: Changing modify acls to: honghuang,
                                                  //| 14/11/20 19:33:59 INFO SecurityManager: SecurityManager: authentication disa
                                                  //| bled; ui acls disabled; users with view permissions: Set(honghuang, ); users
                                                  //|  with modify permissions: Set(honghuang, )
                                                  //| 14/11/20 19:34:00 INFO Slf4jLogger: Slf4jLogger started
                                                  //| 14/11/20 19:34:00 INFO Remoting: Starting remoting
                                                  //| 14/11/20 19:34:01 INFO Remoting: Remoting started; listening on addresses :[
                                                  //| akka.tcp://sparkDriver@10.0.2.15:46538]
                                                  //| 14/11/20 19:34:01 INFO Remoting: Rem
                                                  //| Output exceeds cutoff limit.
//sc.wholeTextFiles("/home/honghuang/t").values.flatMap(_.split("\n\n").grouped(4).map(_.mkString("\n")))

//another thing
    val v=sc.wholeTextFiles("/home/honghuang/t").values.flatMap(_.split("\n\n").map(_.mkString(" ")))
                                                  //> 14/11/20 19:34:07 INFO MemoryStore: ensureFreeSpace(199835) called with curM
                                                  //| em=0, maxMem=541883105
                                                  //| 14/11/20 19:34:07 INFO MemoryStore: Block broadcast_0 stored as values in me
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
                                                  //> 14/11/20 19:34:08 INFO FileInputFormat: Total input paths to process : 1
                                                  //| 14/11/20 19:34:08 INFO FileInputFormat: Total input paths to process : 1
                                                  //| 14/11/20 19:34:08 INFO CombineFileInputFormat: DEBUG: Terminated node alloc
                                                  //| ation with : CompletedNodes: 1, size left: 0
                                                  //| r  : org.apache.spark.rdd.RDD[(String, String)] = MappedRDD[7] at map at te
                                                  //| st.scala:40
      
    
    matchactor.collect().foreach(f=>println(f))   //> 14/11/20 19:34:08 INFO SparkContext: Starting job: collect at test.scala:43
                                                  //| 
                                                  //| 14/11/20 19:34:08 INFO DAGScheduler: Got job 0 (collect at test.scala:43) w
                                                  //| ith 1 output partitions (allowLocal=false)
                                                  //| 14/11/20 19:34:08 INFO DAGScheduler: Final stage: Stage 0(collect at test.s
                                                  //| cala:43)
                                                  //| 14/11/20 19:34:08 INFO DAGScheduler: Parents of final stage: List()
                                                  //| 14/11/20 19:34:08 INFO DAGScheduler: Missing parents: List()
                                                  //| 14/11/20 19:34:08 INFO DAGScheduler: Submitting Stage 0 (FlatMappedRDD[3] a
                                                  //| t flatMap at test.scala:18), which has no missing parents
                                                  //| 14/11/20 19:34:09 INFO MemoryStore: ensureFreeSpace(2560) called with curMe
                                                  //| m=199835, maxMem=541883105
                                                  //| 14/11/20 19:34:09 INFO MemoryStore: Block broadcast_1 stored as values in m
                                                  //| emory (estimated size 2.5 KB, free 516.6 MB)
                                                  //| 14/11/20 19:34:09 INFO DAGScheduler: Submitting 1 missing tasks from Stage 
                                                  //| 0 (FlatMappedRDD[3] at flatMap at test.scala:18)
                                                  //| 14/11/20 19:34:09 INFO
                                                  //| Output exceeds cutoff limit.
    r.filter{case (k,v)=>k!=v}.foreach(f=>println(f))
                                                  //> 14/11/20 19:34:10 INFO SparkContext: Starting job: foreach at test.scala:44
                                                  //| 
                                                  //| 14/11/20 19:34:10 INFO DAGScheduler: Registering RDD 3 (flatMap at test.sca
                                                  //| la:18)
                                                  //| 14/11/20 19:34:10 INFO DAGScheduler: Registering RDD 3 (flatMap at test.sca
                                                  //| la:18)
                                                  //| 14/11/20 19:34:10 INFO DAGScheduler: Got job 1 (foreach at test.scala:44) w
                                                  //| ith 1 output partitions (allowLocal=false)
                                                  //| 14/11/20 19:34:10 INFO DAGScheduler: Final stage: Stage 1(foreach at test.s
                                                  //| cala:44)
                                                  //| 14/11/20 19:34:10 INFO DAGScheduler: Parents of final stage: List(Stage 2, 
                                                  //| Stage 3)
                                                  //| 14/11/20 19:34:10 INFO DAGScheduler: Missing parents: List(Stage 2, Stage 3
                                                  //| )
                                                  //| 14/11/20 19:34:10 INFO DAGScheduler: Submitting Stage 2 (FlatMappedRDD[3] a
                                                  //| t flatMap at test.scala:18), which has no missing parents
                                                  //| 14/11/20 19:34:10 INFO MemoryStore: ensureFreeSpace(2984) called with curMe
                                                  //| m=202395, maxMem=541883105
                                                  //| 14/11/20 19:34:10 INFO MemoryStore: Block broadcast_2 stor
                                                  //| Output exceeds cutoff limit./

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