package com.release.etl.release.dw

import scala.collection.mutable.ArrayBuffer

/**
  * DW 获取日志字段
  */
object DWReleaseColumnsHelper {

  /**
    *
    */
  def selectDWReleaeColumns() = {
    val columns: ArrayBuffer[String] = new ArrayBuffer[String]()
    columns.+=("release_session ")
    columns.+=("release_status ")
    columns.+=("device_num ")
    columns.+=("device_type ")
    columns.+=("sources ")
    columns.+=("channels ")
    columns.+=("get_json_object(exts,\"$.idcard\") idcard ")
    columns.+=("(cast(date_format(now(),'yyyy')as int)-cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\d{6})(\\d{4})',2)as int))as age ")
    columns.+=("(cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\d{16})(\\d{1})',2)as int )%2) as gender ")
    columns.+=("get_json_object(exts,\"$.area_code\") area_code ")
    columns.+=("get_json_object(exts,\"$.longitude\") longitude ")
    columns.+=("get_json_object(exts,\"$.latitude\") latitude ")
    columns.+=("get_json_object(exts,\"$.matter_id\") matter_id ")
    columns.+=("get_json_object(exts,\"$.model_code\") model_code ")
    columns.+=("get_json_object(exts,\"$.model_version\") model_version ")
    columns.+=("get_json_object(exts,\"$.aid\") aid ")
    columns.+=("ct ")
    columns.+=("bdp_day ")

    columns
  }

}