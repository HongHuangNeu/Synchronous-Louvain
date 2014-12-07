package tt

import java.io.FileWriter
import java.util.Calendar

object Logger {

  var time=Calendar.getInstance().getTime()
  var fw :FileWriter= null
  var rs:FileWriter= null
  var i=1
  def initialLog(i:Int)={
    fw = new FileWriter(Setting.graphHome + "/log" +time+"level"+i, true)
    rs= new FileWriter(Setting.graphHome + "/result" +time+"level"+i, true)
  }
  def writeLog(line: String) = {
    println(line)
    //fw.write(line + "\n")
  }
  def terminates() = {
    fw.flush()
    fw.close()
    rs.flush()
    rs.close()
  }
  def close() = {
    fw.flush()
    fw.close()
    rs.flush()
    rs.close()
  }
  //to be removed
  def writeAdditionalLog(line:String)={
    println(line)
    fw.write("the"+i+"level ends")
    
    fw.write(line + "\n")
  }
  def writeResult(line:String)={
    rs.write(line+"\n")
  }
}