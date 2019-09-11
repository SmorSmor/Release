package com.release.constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * 常用工具类
  */
object ReleaseConstant {

  // 分区参数
  val DEF_STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_AND_DISK
  val DEF_SAVEMODE: SaveMode = SaveMode.Overwrite
  val DEF_PARTITION: String = "bdp_day"
  val DEF_SOURCE_PARTITIONS = 4


  // ods-----------------------
  val ODS_RELEASE_SESSION: String = "ods_release.ods_01_release_session"
  // dw------------------------
  val DW_RELEASE_CUSTOMER: String = "dw_release.dw_release_customer"
  val DW_RELEASE_REGISTER: String = "dw_release.dw_release_register_users"
  val DW_RELEASE_EXPOSURE: String = "dw_release.dw_release_exposure"

  // dm------------------------
  val DM_RELEASE_CUSTOMER: String = "dm_release.dm_customer_cube"
  val DM_RELEASE_EXPOSURE: String = "dm_release.dm_exposure_sources"

  // 维度列
  val COL_RELEASE_SESSION_STATUS:String = "release_status"
  val COL_RELEASE_SOURCES = "sources"
  val COL_RELEASE_CHANNELS = "channels"
  val COL_RELEASE_DEVICE_TYPE = "device_type"
  val COL_RELEASE_DEVICE_NUM = "device_num"
  val COL_RELEASE_USER_COUNT = "user_count"
  val COL_RELEASE_TOTAL_COUNT = "total_count"
  val COL_RELEASE_AGE_RANGE = "age_range"
  val COL_RELEASE_GENDER = "gender"
  val COL_RELEASE_AREA_CODE = "area_code"
  val COL_RELEASE_SESSION = "release_session"
  val COL_RELEASE_EXPOSURE_COUNT = "exposure_count"
  val COL_RELEASE_EXPOSURE_RATES = "exposure_rates"



}
