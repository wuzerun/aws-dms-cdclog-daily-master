package cn.esquel.dataLake

//import kafka.utils.Logging
import com.sun.javafx.util.Logging
import org.apache.log4j.{Level, Logger}
//import org.apache.spark.Logging

object LoggerLevels extends Logging {

  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
//      logInfo("Setting log level to [WARN] for streaming example." +
//        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}