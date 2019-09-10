package com.release.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StringType
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
    * 写入数据
    */
  def writeTableData(sourceDF: DataFrame, tableName: String, mode: SaveMode) = {
    // 回去当前时间
    val begin = System.currentTimeMillis()
    // 读取表
    sourceDF.write.mode(mode).insertInto(tableName)
    println(s"table[${tableName}] use ; ${System.currentTimeMillis() - begin}}")

  }

  /**
    * 创建spark上下文
    */
  def createSpark(sconf: SparkConf) = {
    val spark: SparkSession = SparkSession
      .builder()
      .config(sconf)
      .enableHiveSupport()
      .getOrCreate()
    spark

  }

  /**
    * UDF 注册
    */
  def registerFun(spark: SparkSession,UDFName:String,UDF: UDF1[_,_])={
    spark.udf.register(UDFName,UDF,_)

  }

  /**
    *  参数校验
    */
def rangeDates(begin:String,end:String)={

  val bdp_days = new ArrayBuffer[String]()
  try{

  }catch {
    case ex: Exception => {
      logger.error(ex.getMessage, ex)
    }
  }



}


}