package com.release.etl.release.dw

import com.release.constant.ReleaseConstant
import com.release.enums.ReleaseStatusEnum
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import com.release.util.SparkHelper
import org.apache.spark.SparkConf

/**
  * DW 投放目标客户
  */
object DWReleaseCustomer {

  // 日志处理
  val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

  /**
    * 目标客户
    * status = “01”
    */
  def handleReleaseJob(spark: SparkSession, appName: String, bdp_day: String) = {
    // 回去当前时间
    val begin = System.currentTimeMillis()


    try {
      // 导入隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._
      // 设置缓存级别
      val storageleavel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val savemode: SaveMode = ReleaseConstant.DEF_SAVEMODE
      // 获取当天字段的数据
      val customerColumns = DWReleaseColumnsHelper.selectDWReleaeColumns()

      val customerColumnStatus = (col(s"${ReleaseConstant.DEF_PARTITION}")) === lit(bdp_day) and
        col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit(ReleaseStatusEnum.CUSTOMER.getCode)
      // 读取数据
      val customerReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.ODS_RELEASE_SESSION, customerColumns)
        // 查询条件
        .where(customerColumnStatus)
      // 重分区
      // .repartition(ReleaseConstant.DEF_SOURCE_PARTITIONS)
      println("查询结束--------------------------------------------------结果显示")
      customerReleaseDF.show(10, false)
      SparkHelper.writeTableData(customerReleaseDF, ReleaseConstant.DW_RELEASE_CUSTOMER, savemode)

    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage, ex)
      }
    }


  }

  def handleJob(appName: String, bdp_day: String, bdp_day_end: String) = {

    var spark: SparkSession = null

    try {
      // spark配置参数
      val conf: SparkConf = new SparkConf()
        .setAppName(appName)
        .setMaster("local[*]")
      // spark上下文
      spark = SparkHelper.createSpark(conf)
      // 参数校验


    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage, ex)
      }
    } finally {
      if (spark != null)
        spark.close()
    }

  }

  def main(args: Array[String]): Unit = {

  }
}