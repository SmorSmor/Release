package com.release.etl.release.dm

import com.release.constant.ReleaseConstant
import com.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

object DMReleaseExposure {
  // 日志处理
  val logger: Logger = LoggerFactory.getLogger(DMReleaseExposure.getClass)

  /**
    * 曝光主题
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
      val exposureColumns = DMReleaseColumnsHelper.selectDMReleaseExposureColumns()

      val exposureColumnStatus = (col(s"${ReleaseConstant.DEF_PARTITION}")) === lit(bdp_day)
      val exposureGroupColumns =
        Seq[Column](
          $"${ReleaseConstant.COL_RELEASE_SOURCES}",
          $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
          $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
          $"total"
        )

      // 读取数据

      val total: Long = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_EXPOSURE).count()

      val exposureReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_EXPOSURE)
        // 查询条件
        .where(exposureColumnStatus)
        .selectExpr(exposureColumns: _*)
        .withColumn("total", lit(total))
        .groupBy(exposureGroupColumns: _*)
        .agg(
          count(s"${ReleaseConstant.COL_RELEASE_SESSION}").alias("exposure_count")
        )
        .selectExpr("sources", "channels", "device_type", "exposure_count", "exposure_count/total as exposure_rates")

        .withColumn(ReleaseConstant.DEF_PARTITION, lit(bdp_day))
        // 重分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITIONS)
      println("查询结束--------------------------------------------------结果显示")
      exposureReleaseDF.show(10, false)
      //      SparkHelper.writeTableData(exposureReleaseDF, ReleaseConstant.DM_RELEASE_EXPOSURE, savemode)

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
    val appName: String = "dm_release_exposure_job"
    val bdp_day_begin: String = "2019-09-09"
    val bdp_day_end: String = "2019-09-09"
    // 执行Job
    handleJob(appName, bdp_day_begin, bdp_day_end)

  }
}