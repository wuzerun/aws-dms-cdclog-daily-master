package cn.esquel.dataLake

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
/*
* 从s3n://esq-dms-staging-prd读取数据到s3n://esq-dms-event-log-prd(parquet文件，按天存储)
* 其中fullLoad文件名格式: FULLLOAD-20180602-154921000-LOAD00000001.csv
* 从s3n://esq-dms-staging-prd/ESCM_CEG_CEK/ESCMOWNER/AGPO_HD/20180620-082810410.csv
* 到s3n://esq-dms-event-log-prd/ESCM_CEG_CEK/ESCMOWNER/AGPO_HD/cdc_year=2018/cdc_month=6/cdc_day=10/...parquet
* 说明：每次循环处理一个表的新增的csv格式的cdclog,在S3表路径下批量加载，并以parquet格式按天保存成一个文件
*/
object StagingToEventLogApp {
  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels();

    val staging: StagingToEventLogApp = new StagingToEventLogApp
    val startTime = staging.NowTime
    val conf = new SparkConf().setAppName("StagingToEventLog")
      .setMaster("local[*]")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.sql.broadcastTimeout","1000")
//    .set("spark.sql.parquet.cacheMetadata", "true")
//    .set("spark.local.dir", "D://sparkTemp")//spark的缓存路径
//    .set("spark.driver.memory","4g")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val AccessKeyId = ""
    val SecretAccessKey = ""
    val inputPath = "s3n://esq-dms-staging-prd"
    val outputPath = "s3n://esq-dms-event-log-prd"

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AccessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", SecretAccessKey)

    staging.operationCDCLog(inputPath, sc, spark, outputPath)
    val endTime = staging.NowTime
    println("Start Time: " + startTime)
    println("End Time: " + endTime)
  }
}

class StagingToEventLogApp extends Serializable {

  def operationCDCLog(inputPath:String, sc:SparkContext, spark:SparkSession, outputPath:String): Unit= {
    val fs = FileSystem.get(new java.net.URI(inputPath), sc.hadoopConfiguration)
    if (fs.exists(new Path(inputPath))) {
      val files: Array[FileStatus] = fs.listStatus(new Path(inputPath))
      import util.control.Breaks._
      breakable(
        if (files != null) {
          files.foreach { case (file) =>
            if (file.isFile) {
              val sourcePath = file.getPath.toString
              val cdcLogPathPref = sourcePath.substring(0, sourcePath.lastIndexOf("/"))
              val outPath = cdcLogPathPref.replace(cdcLogPathPref.split("/").toList(2), outputPath.split("/").toList(2))
              fullLoadCSV(fs, cdcLogPathPref, outPath, spark)
              println("inputPath: " + cdcLogPathPref)
              cdcToEventLog(spark, cdcLogPathPref, outPath)
              deleteS3AfterSave(cdcLogPathPref, sc)
              break()
            } else if (file.isDirectory) {
              operationCDCLog(file.getPath.toString, sc, spark, outputPath)
            } else {
              println("This isn't a directory or file")
              println("fileName: " + file.getPath.toString)
            }
          }
        }
      )
    } else {
      println("no exists path on s3: " + inputPath)
    }
  }

  def fullLoadCSV(fs: FileSystem, cdcLogPathPref:String, outPath:String, spark:SparkSession): Unit={
    val fullLoadFiles: Array[FileStatus] = fs.listStatus(new Path(cdcLogPathPref)).filter(_.getPath.toString.contains("LOAD00000001.csv"))
    if(fullLoadFiles!=null){
      fullLoadFiles.foreach { case (file) =>
        val fullLoadFilePath = file.getPath.toString
        println("fullLoad inputPath: " + fullLoadFilePath)
        fullLoadToEventLog(spark, fullLoadFilePath, outPath)
        deleteS3AfterSave(fullLoadFilePath, spark.sparkContext)
      }
    }
  }

  def fullLoadToEventLog(spark: SparkSession, path:String, outPath:String): Unit = {
    //.option("escape","\n").option("multiLine",true)这两个属性解决了.csv文件的单个字段中存在换行造成读取数据错误的问题
    var fullLoadDF: DataFrame = spark.read.option("sep", ",")
      .option("quote","\"")
      .option("escape","\n")
      .option("multiLine",true)
      .option("header", true)
      .option("inferSchema", false)
      .csv(path)
    fullLoadDF = fullLoadDF.withColumn("Op",lit("L"))
    fullLoadDF = modifyColumns(fullLoadDF)
    fullLoadDF.coalesce(1)
      .write.partitionBy("cdc_year","cdc_month","cdc_day")
      .mode(SaveMode.Append)
      .parquet(outPath)
  }

  def cdcToEventLog(spark: SparkSession, path:String, outPath:String): Unit = {
    var cdcDF: DataFrame = spark.read.option("sep", ",")
      .option("quote","\"")
      .option("escape","\n")
      .option("multiLine",true)
      .option("header", true)
      .option("inferSchema", false)
      .csv(path)
    cdcDF = modifyColumns(cdcDF)
    cdcDF.coalesce(1)
      .write.partitionBy("cdc_year","cdc_month","cdc_day")
      .mode(SaveMode.Append)
      .parquet(outPath)
  }

  def modifyColumns(df: DataFrame): DataFrame = {
    var dfs = df.withColumn("cdc_filename", input_file_name)
    dfs = dfs.withColumn("cdc_file_seq",getHashUdf(dfs.col("cdc_filename"), monotonically_increasing_id))
    dfs = dfs.withColumn("cdc_last_modified", dateTimeUdf(dfs.col("cdc_filename")))
    dfs = dfs.withColumn("cdc_year", year(dfs.col("cdc_last_modified")))
    dfs = dfs.withColumn("cdc_month", month(dfs.col("cdc_last_modified")))
    dfs = dfs.withColumn("cdc_day", dayofmonth(dfs.col("cdc_last_modified")))
    dfs
  }

  def deleteS3AfterSave(inputPath: String,sc: SparkContext): Unit={
    FileSystem.get(new java.net.URI(inputPath), sc.hadoopConfiguration)
      .delete(new Path(inputPath),true)
  }

  def getHashUdf = udf((fileName:String,seq:Int)=> {
    var fileSeq = ""
    val str = strSplit(fileName)
    if(fileName.contains("LOAD00000001.csv")){
      val arr: Array[String] = str(str.length-1).split("-")
      fileSeq = arr(1) + "-" + arr(2) + "%064d".format(seq)//64位整数序列
    } else{
      fileSeq = str(str.length-1).replace(".csv","") + "%064d".format(seq)
    }
    fileSeq
  })

  def dateTimeUdf= udf((fileName: String)=>{
    //s3n://esq-dms-staging-prd/ESCM_CEG_CEK/ESCMOWNER/AGPO_HD/20180620-082810410.csv
    //s3n://esq-dms-staging-prd/ESCM_CEG_CEK/ESCMOWNER/AGPO_HD/FULLLOAD-20180602-154921000-LOAD00000001.csv
    var s = ""
    val df = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val str = strSplit(fileName)
    if(fileName.contains("LOAD00000001.csv")){
      val arr: Array[String] = str(str.length-1).split("-")
      s = arr(1) + arr(2)
    } else {
      val arr: Array[String] = str(str.length-1).split("-")
      s = arr(0) + arr(1)
    }
    val date: Date = df.parse(s)
    val dateTime = df1.format(date)
    dateTime
  })

  def strSplit(fileName:String):Array[String]={
    fileName.split("/")
  }

  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
    val date = dateFormat.format(now)
    return date
  }

  def NowTime(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }
}



