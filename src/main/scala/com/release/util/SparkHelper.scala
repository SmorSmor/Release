package com.release.util

import com.release.udf.QFUdf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * 工具类
  */
object SparkHelper {
  // 处理日志
  val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

  /**
    * 读取数据表
    */
  def readTableData(spark: SparkSession, tableName: String, colNames: mutable.Seq[String]) = {
    // 回去当前时间
    val begin = System.currentTimeMillis()
    // 读取表
    val tableDF = spark.read.table(tableName).selectExpr(colNames: _*)

    tableDF
  }

  /**
    * 读取数据表
    */
  def readTableData(spark: SparkSession, tableName: String) = {
    // 回去当前时间
    val begin = System.currentTimeMillis()
    // 读取表
    val tableDF = spark.read.table(tableName)

    tableDF
  }

  /**
    * 写入数据
    */
  def writeTableData(sourceDF: DataFrame, tableName: String, mode: SaveMode) = {
    // 回去当前时间
    val begin = System.currentTimeMillis()
    // 读取表
    sourceDF.write.mode(mode).insertInto(tableName)
    println(s"table[${tableName}] use : ${System.currentTimeMillis() - begin}-==========")

  }

  /**
    * 创建spark上下文
    */
  def createSpark(sconf: SparkConf): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .config(sconf)
      .enableHiveSupport()
      .getOrCreate()
    // 为了处理目标主题下面的DM层统计 年龄段做准备
    registerFun(spark)

    spark
  }

  /**
    * UDF 注册
    */
  def registerFun(spark: SparkSession): Unit = {
    // 处理年龄段
    spark.udf.register("getAgeRange", QFUdf.getAgeRange _)

    spark.udf.register("getScore",QFUdf.getScoue _)

    spark.udf.register("getLevel",QFUdf.getLevel _)


  }

  /**
    * 参数校验
    */
  def rangeDates(begin: String, end: String): Seq[String] = {
    val bdp_days = new ArrayBuffer[String]()
    try {
      val bdp_date_begin = DateUtil.dateFormat4String(begin, "yyyy-MM-dd")
      val bdp_date_end = DateUtil.dateFormat4String(end, "yyyy-MM-dd")

      if (begin.equals(end)) {
        bdp_days.+=(bdp_date_begin)
      } else {
        var cday = bdp_date_begin
        while (cday != bdp_date_end) {
          bdp_days.+=(cday)
          val pday = DateUtil.dateFormat4StringDiff(cday, 1)
          cday = pday
        }
      }
    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage, ex)
      }
    }
    bdp_days
  }


}