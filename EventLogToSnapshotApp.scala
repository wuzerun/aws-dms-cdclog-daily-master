package cn.esquel.dataLake

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import scala.collection.mutable.ArrayBuffer

/*
 * 从s3n://esq-dms-event-log-prd读取数据到s3n://esq-dms-db-snapshot-prd
 * 从s3n://esq-dms-event-log-prd/ESCM_CEG_CEK/ESCMOWNER/AGPO_HD/...csv
 * 到s3n://esq-dms-db-snapshot-prd/ESCM_CEG_CEK/ESCMOWNER/AGPO_HD/cdc_snapshot_date=2018-06-02/...parquet
 * 说明：每个循环处理一个表，每个表读写一次，假如每天都运行的话，相当于每天都要读写一次整个数据库的数据
 */
object EventLogToSnapshotApp {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels();

    val snapshot = new EventLogToSnapshotApp()
    val startTime = snapshot.NowDate

    val conf = new SparkConf().setAppName("EventLogToSnapshot")
      .setMaster("local[*]")
      .set("spark.debug.maxToStringFields", "100")
//    .set("spark.local.dir", "D://sparkTemp") //spark的缓存路径
      .set("spark.sql.broadcastTimeout", "1000") //广播设置为1000ms

    val spark = SparkSession.builder().config(conf).getOrCreate();

    val AccessKeyId = "";
    val SecretAccessKey = "";
    val inputPath = "s3n://esq-dms-event-log-prd"
    val outputPath = "s3n://esq-dms-db-snapshot-prd"

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AccessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", SecretAccessKey)

    val arr: ArrayBuffer[String] = new ArrayBuffer[String]
    snapshot.operationEventLog(inputPath, sc, spark, outputPath, arr)

    val endTime = snapshot.NowDate
    println("Start Time:" + startTime)
    println("End Time:" + endTime)
  }

}

class EventLogToSnapshotApp extends Serializable {

  def operationEventLog(inputPath: String, sc: SparkContext, spark: SparkSession, outputPath: String, arr: ArrayBuffer[String]): Unit = {

    val fs = FileSystem.get(new java.net.URI(inputPath), sc.hadoopConfiguration)
    if (fs.exists(new Path(inputPath))) {
      val files: Array[FileStatus] = fs.listStatus(new Path(inputPath))
      import util.control.Breaks._
      breakable(
        files.foreach { case (file) =>
          if (file.isDirectory && file.getPath.toString.contains("/cdc_year=")) {
            val sourFile = file.getPath.toString
            val path = sourFile.substring(0, sourFile.indexOf("/cdc_year="))
            println("inputPath:  " + path)
            //只获取最后的文件夹，构建文件访问路径，即每个表的S3路径
            //获取总文件夹路径 s3n://esq-datalake-parquet-prd/ESCM_CEG_CEK/ESCMOWNER/AGPO_HD
            val outPath = path.replace(path.split("/").toList(2), outputPath.split("/").toList(2))
            buildLatestDf(spark, outPath, path)
//          deleteS3AfterSave(path, sc)
            break()
          } else if (file.isDirectory) {
            operationEventLog(file.getPath.toString, sc, spark, outputPath, arr)
          }
        }
      )
    } else {
      println("no exists path on s3: " + inputPath)
    }
  }

  def buildLatestDf(spark: SparkSession, outputPath: String, path: String): Unit = {
    //必须检查输出路径下是否存在parquet文件,因为在上一次在S3输出时报错会有中间文件，只单纯判断路径是否存在会使下一步报错
    var flag = false
    flag = isParquetFile(outputPath, spark)
    if (flag) {
      //获取前一天的Snapshot
      var lastSnapshotDf: DataFrame = spark.read.option("sep", ",")
        .option("quote", "\"")
        .option("escape", "\n")
        .option("multiLine", true)
        .option("header", true)
        .option("inferSchema", false)
        .parquet(outputPath)

      lastSnapshotDf = lastSnapshotDf.filter(lastSnapshotDf("Op") === "S"
        && lastSnapshotDf("cdc_snapshot_date") === getYesterday)

      if (lastSnapshotDf.limit(10).count > 0) {
        val time: Array[String] = requestDate.split("-")
        var snapshotDateCdcDf: DataFrame = dateCdcDf(path, spark)
        //只取今天的cdclog
        snapshotDateCdcDf = snapshotDateCdcDf.filter(snapshotDateCdcDf("cdc_year") === time(0).toInt
          && snapshotDateCdcDf("cdc_month") === time(1).toInt && snapshotDateCdcDf("cdc_day") === time(2).toInt)
        reduceToSnapshot(path, outputPath, lastSnapshotDf.drop("cdc_snapshot_date").union(snapshotDateCdcDf), spark)
      } else {
        val snapshotDateCdcDf: DataFrame = dateCdcDf(path, spark)
        reduceToSnapshot(path, outputPath, snapshotDateCdcDf, spark)
      }
    } else {
      val snapshotDateCdcDf: DataFrame = dateCdcDf(path, spark)
      reduceToSnapshot(path, outputPath, snapshotDateCdcDf, spark)
    }
  }

  def reduceToSnapshot(path: String, outputPath: String, df: DataFrame, spark: SparkSession): Unit = {
    if (df != null) {
      val str = path.split("/")
      val tableName = str(str.length - 1)
      //获取表的主键部分可以放在代码的开头，并且把它卸载缓存中而不需要每个循环都读取，这里暂且放在这里
      val pkDF = getPkDF(spark)
      val columnNameArr: Array[Row] = pkDF.filter(pkDF("TABLE_NAME") === tableName).select(pkDF("COLUMN_NAME")).collect

      if (!columnNameArr.isEmpty) {
        val pkArray = columnNameArr.mkString(",").replace("[", "").replace("]", "").split(",")
        //根据主键去重
        val latestRowDf = df.groupBy(pkArray.head, pkArray.tail: _*) //pkArray中同一个表可能有多个主键，所以用groupBy的参数才这么写
          .agg(max(df("cdc_file_seq")).alias("last_seq"))
          .alias("max")
          .select("max.last_seq")
          .dropDuplicates
          .sort("last_seq")
        //拿到新增的且去重后的cdclog文件与前一天的整张表的记录进行right join，每天都运行的话，相当于每天都要读写一次整个数据库的数据
        val latestDf = df.join(latestRowDf, df("cdc_file_seq") === latestRowDf("last_seq"), "right")
          .drop("last_seq")
          .withColumn("cdc_snapshot_date", lit(requestDate))
          .filter(df("Op").isin("L", "S", "I", "U"))
          .withColumn("Op", lit("S"))
        //写入S3
        latestDf.coalesce(1)
          .sortWithinPartitions(pkArray.head, pkArray.tail: _*)
          .write.partitionBy("cdc_snapshot_date")
          .mode(SaveMode.Append)
          .parquet(outputPath)

      } else {
        val latestDf = df.withColumn("cdc_snapshot_date", lit(requestDate))
          .filter(df("Op").isin("L", "S", "I", "U"))
          .withColumn("Op", lit("S"))
        latestDf.coalesce(1)
          .write.partitionBy("cdc_snapshot_date")
          .mode(SaveMode.Append)
          .parquet(outputPath)
      }
    }
  }

  def dateCdcDf(path: String, spark: SparkSession): DataFrame = {
    var cdcDf: DataFrame = null
    val flag = isParquetFile(path, spark)
    if (flag) {
      cdcDf = spark.read.option("sep", ",")
        .option("quote", "\"")
        .option("escape", "\n")
        .option("multiLine", true)
        .option("header", true)
        .option("inferSchema", false)
        .parquet(path)
    } else {
      println("no exite parquet files: " + path)
    }
    cdcDf
  }

  def deleteS3AfterSave(inputPath: String, sc: SparkContext): Unit = {
    FileSystem.get(new java.net.URI(inputPath), sc.hadoopConfiguration).delete(new Path(inputPath), true)
  }

  def getPkDF(spark: SparkSession): DataFrame = {
    val pkDF: DataFrame = spark.read.option("sep", ",")
      .option("quote", "\"")
      .option("escape", "\n")
      .option("multiLine", true)
      .option("header", true)
      .option("inferSchema", false)
      .csv("D://sparktest/config/PK.csv")
    pkDF
  }

  def isParquetFile(path: String, spark: SparkSession): Boolean = {
    var flag = false
    val fs = FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration)
    import util.control.Breaks._
    val dir = fs.exists(new Path(path))
    breakable(
      if (dir) {
        val files = fs.listStatus(new Path(path))
        if (!files.isEmpty) {
          for (file <- files) {
            if (file.isDirectory) {
              if (flag) {
                break()
              } else {
                flag = isParquetFile(file.getPath.toString, spark)
              }
            } else if (file.isFile && file.getPath.toString.endsWith(".parquet")) {
              flag = true
              break()
            } else {
              flag = false
            }
          }
          break()
        }
      } else {
        flag = false
      }
    )
    flag
  }

  def snapshotDateUdf = udf((year: String, month: String, day: String) => {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val str = year + "-" + month + "-" + day
    val date: Date = df.parse(str)
    val dateTime = df.format(date)
    dateTime
  })

  def requestDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.format(now)
    return date
  }

  def getYesterday(): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }
}