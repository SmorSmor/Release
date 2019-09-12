package com.release.udf

import com.release.util.CommonUtil

/**
  * spark UDF
  */
object QFUdf {

  /**
    * 年龄段
    */
  def getAgeRange(age: String): String = {
    var tseg = ""
    if (null != age) {
      try {
        tseg = CommonUtil.getAgeRange(age)
      } catch {
        case ex: Exception => {
          println(s"$ex")
        }
      }
    }
    tseg
  }


  def getScoue(actions: String): Int = {
    actions match {
      case "01" => 1
      case "02" => 2
      case "03" => 2
      case "04" => 3
      case "05" => 5
      case _ => 0
    }
  }
//用户等级【普通(新用户)、银牌(用户积分[1000-3000)、金牌[3000-10000)、钻石(10000+)】
  def getLevel(score :Int) ={
    var level ="普通"
    if(score>1000 && score < 3000)
      level = "银牌"

  if(score>3000 && score < 10000)
      level = "金牌"

  if(score>10000)
      level = "钻石"
    level

  }

}
