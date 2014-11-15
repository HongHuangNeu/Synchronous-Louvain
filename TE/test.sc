
object test {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
val conf = new SparkConf().setAppName("hello").setMaster("local")
                                                  //> conf  : org.apache.spark.SparkConf = org.apache.spark.SparkConf@24f157b0
val sc=new SparkContext(conf)                     //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| 14/11/13 16:43:36 WARN Utils: Your hostname, honghuang-VirtualBox resolves t
                                                  //| o a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface eth0)
                                                  //| 
                                                  //| 14/11/13 16:43:36 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to anot
                                                  //| her address
                                                  //| 14/11/13 16:43:36 INFO SecurityManager: Changing view acls to: honghuang,
                                                  //| 14/11/13 16:43:36 INFO SecurityManager: Changing modify acls to: honghuang,
                                                  //| 14/11/13 16:43:36 INFO SecurityManager: SecurityManager: authentication disa
                                                  //| bled; ui acls disabled; users with view permissions: Set(honghuang, ); users
                                                  //|  with modify permissions: Set(honghuang, )
                                                  //| 14/11/13 16:43:37 INFO Slf4jLogger: Slf4jLogger started
                                                  //| 14/11/13 16:43:37 INFO Remoting: Starting remoting
                                                  //| 14/11/13 16:43:38 INFO Remoting: Remoting started; listening on addresses :[
                                                  //| akka.tcp://sparkDriver@10.0.2.15:56999]
                                                  //| 14/11/13 16:43:38 INFO Remoting: Rem
                                                  //| Output exceeds cutoff limit.
val lines = sc.textFile("/home/honghuang/data.txt")
                                                  //> 14/11/13 16:43:45 INFO MemoryStore: ensureFreeSpace(152566) called with curM
                                                  //| em=0, maxMem=541883105
                                                  //| 14/11/13 16:43:45 INFO MemoryStore: Block broadcast_0 stored as values in me
                                                  //| mory (estimated size 149.0 KB, free 516.6 MB)
                                                  //| lines  : org.apache.spark.rdd.RDD[String] = /home/honghuang/data.txt MappedR
                                                  //| DD[1] at textFile at test.scala:9
val pairs = lines.map(s => (s, 1))                //> pairs  : org.apache.spark.rdd.RDD[(String, Int)] = MappedRDD[2] at map at te
                                                  //| st.scala:10
val counts = pairs.reduceByKey((a, b) => a + b)   //> 14/11/13 16:43:46 INFO FileInputFormat: Total input paths to process : 1
                                                  //| counts  : org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[3] at reduce
                                                  //| ByKey at test.scala:11
  println("the line is "+counts)                  //> the line is ShuffledRDD[3] at reduceByKey at test.scala:11

}