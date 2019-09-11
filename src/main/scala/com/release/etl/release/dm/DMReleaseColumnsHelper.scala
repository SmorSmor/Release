package com.release.etl.release.dm


import scala.collection.mutable.ArrayBuffer

object DMReleaseColumnsHelper {
  /**
    * DM 客户集市
     */
  def selectDMReleaseCustomerColumns(): ArrayBuffer[String] = {
    val columns: ArrayBuffer[String] = new ArrayBuffer[String]()
    columns.+=("sources ")
    columns.+=("channels ")
    columns.+=("device_type ")
    columns.+=("getAgeRange(age) as age_range")
    columns.+=("gender ")
    columns.+=("area_code ")
    columns.+=("device_num ")
    columns.+=("release_session ")
    columns.+=("bdp_day ")

    columns
  }


  /**
    * DM 曝光集市
     */
  def selectDMReleaseExposureColumns(): ArrayBuffer[String] = {
    val columns: ArrayBuffer[String] = new ArrayBuffer[String]()
    columns.+=("sources ")
    columns.+=("release_session ")
    columns.+=("device_num ")
    columns.+=("channels ")
    columns.+=("device_type ")
    columns.+=("bdp_day ")

    columns
  }

}
