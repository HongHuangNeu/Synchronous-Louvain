package tt

import java.io.FileWriter
import java.util.Calendar

object Logger {

  val fw = new FileWriter(Setting.graphHome + "/log" + Calendar.getInstance().getTime(), true)

  def writeLog(line: String) = {
    println(line)
    fw.write(line + "\n")
  }
  def terminates() = {
    fw.flush()
    fw.close()
  }
  def close() = {
    fw.flush()
    fw.close()
  }
}