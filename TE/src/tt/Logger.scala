package tt

import java.io.FileWriter

object Logger {
val fw = new FileWriter("/home/honghuang/log", true)
def writeLog(line:String)={
  println(line)
  fw.write( line+"\n")
}
def close()={
  fw.close()
}
}