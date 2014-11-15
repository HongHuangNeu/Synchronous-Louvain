
object test {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(58); 
  println("Welcome to the Scala worksheet")
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf;$skip(176); 
val conf = new SparkConf().setAppName("hello").setMaster("local");System.out.println("""conf  : org.apache.spark.SparkConf = """ + $show(conf ));$skip(30); 
val sc=new SparkContext(conf);System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(52); 
val lines = sc.textFile("/home/honghuang/data.txt");System.out.println("""lines  : org.apache.spark.rdd.RDD[String] = """ + $show(lines ));$skip(35); 
val pairs = lines.map(s => (s, 1));System.out.println("""pairs  : org.apache.spark.rdd.RDD[(String, Int)] = """ + $show(pairs ));$skip(48); 
val counts = pairs.reduceByKey((a, b) => a + b);System.out.println("""counts  : org.apache.spark.rdd.RDD[(String, Int)] = """ + $show(counts ));$skip(33); 
  println("the line is "+counts)}

}
