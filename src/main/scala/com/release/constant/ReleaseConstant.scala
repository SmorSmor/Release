package com.release.constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * 常用工具类
  */
object ReleaseConstant {

  val DEF_STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_AND_DISK
  val DEF_SAVEMODE: SaveMode = SaveMode.Overwrite
  val DEF_PARTITION: String = "bdp_day"
  val DEF_SOURCE_PARTITIONS = 4

  // 维度列
  val COL_RELEASE_SESSION_STATUS: String = "release_status"

  // ods-----------------------
  val ODS_RELEASE_SESSION: String = "ods_release.ods_01_release_session"
  // dw------------------------
  val DW_RELEASE_CUSTOMER: String = "dw_release.dw_release_customer"


}
