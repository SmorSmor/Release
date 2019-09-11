package com.release.etl.release.dw

import scala.collection.mutable.ArrayBuffer

/**
  * DW 获取日志字段
  */
object DWReleaseColumnsHelper {

  /**
    *
    */
  def selectDWReleaseCustomerColumns():ArrayBuffer[String] = {
    val columns: ArrayBuffer[String] = new ArrayBuffer[String]()
    columns.+=("release_session ")
    columns.+=("release_status ")
    columns.+=("device_num ")
    columns.+=("device_type ")
    columns.+=("sources ")
    columns.+=("channels ")
    columns.+=("get_json_object(exts,'$.idcard') idcard ")
    columns.+=("(cast(from_unixtime(unix_timestamp(),'yyyy')as int)-cast(substr(get_json_object(exts,'$.idcard'),7,4)as int))as age")
    columns.+=("cast(substr(get_json_object(exts,'$.idcard'),17,1)as int )%2 as gender")
    columns.+=("get_json_object(exts,'$.area_code') area_code ")
    columns.+=("get_json_object(exts,'$.longitude') longitude ")
    columns.+=("get_json_object(exts,'$.latitude') latitude ")
    columns.+=("get_json_object(exts,'$.matter_id') matter_id ")
    columns.+=("get_json_object(exts,'$.model_code') model_code ")
    columns.+=("get_json_object(exts,'$.model_version') model_version ")
    columns.+=("get_json_object(exts,'$.aid') aid ")
    columns.+=("ct ")
    columns.+=("bdp_day ")

    columns
  }

  /**
    *
    */
  def selectDWReleaseExposureColumns():ArrayBuffer[String]={
    var columns = new ArrayBuffer[String]()

    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num ")
    columns.+=("device_type")
    columns.+=("sources  ")
    columns.+=("channels  ")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns


  }

  /**
    *
    */
  def selectDWReleaseRegisterColumns():ArrayBuffer[String] = {
    val columns: ArrayBuffer[String] = new ArrayBuffer[String]()
    columns.+=("get_json_object(exts,'$.user_register') user_id ")
    columns.+=("release_session ")
    columns.+=("release_status ")
    columns.+=("device_num ")
    columns.+=("device_type ")
    columns.+=("sources ")
    columns.+=("channels ")
    columns.+=("ct ")
    columns.+=("bdp_day ")

    columns
  }

}