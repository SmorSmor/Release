package com.release.etl.release.dm

import com.release.constant.ReleaseConstant
import com.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

object DMReleaseCustomer {

  // 日志处理
  val logger: Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)

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
      val customerColumns = DMReleaseColumnsHelper.selectDMReleaseCustomerColumns()

      val customerColumnStatus = (col(s"${ReleaseConstant.DEF_PARTITION}")) === lit(bdp_day)
      val customerCubeGroupColumns =
        Seq[Column](
          $"${ReleaseConstant.COL_RELEASE_SOURCES}",
          $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
          $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
          $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
          $"${ReleaseConstant.COL_RELEASE_GENDER}",
          $"${ReleaseConstant.COL_RELEASE_AREA_CODE}"
        )
      // 读取数据
      val customerReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER, customerColumns)
        // 查询条件
        .where(customerColumnStatus)
        .persist(storageleavel)
        .cube(customerCubeGroupColumns:_*)
        .agg(
          // lit() => 获取字面量
          countDistinct($"${ReleaseConstant.COL_RELEASE_DEVICE_NUM}").alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
          count($"${ReleaseConstant.COL_RELEASE_DEVICE_NUM}").alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
        )
        // 添加一列
        .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      println("查询结束--------------------------------------------------结果显示")
      customerReleaseDF.show(200, false)
      //      SparkHelper.writeTableData(customerReleaseDF, ReleaseConstant.DM_RELEASE_CUSTOMER, savemode)

    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage, ex)
      }
    }

  }

  def handleJob(appName: String, bdp_day_begin: String, bdp_day_end: String) = {

    var spark: SparkSession = null

    try {
      // spark配置参数
      val conf: SparkConf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
        .setAppName(appName)
        .setMaster("local[4]")
      // spark上下文
      spark = SparkHelper.createSpark(conf)
      // 参数校验
      val timeRanges = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      for (bdp_day <- timeRanges.reverse) {
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark, appName, bdp_date)
      }

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
    val appName: String = "dm_release_customer_job"
    val bdp_day_begin: String = "2019-09-09"
    val bdp_day_end: String = "2019-09-09"
    // 执行Job
    handleJob(appName, bdp_day_begin, bdp_day_end)

  }
}



