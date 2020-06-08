package com.company.analytics.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.udf

object App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("File path should be used as argument")
      System.exit(-1)
    }

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkApp")
      .getOrCreate()

    //val df = spark.read.option("header", "true").csv("src/main/resources/sample.csv")

    val df = spark.read.option("header", "true").csv(args(0))

    //df.show()

    val getEnglishNameUDF = udf(getEnglishName)
    val getStartYearUDF = udf(getStartYear)
    val getTrendUDF = udf(getTrend)

    df.select(getEnglishNameUDF(df("Species")).as("species"), df("Category").as("category"),
      getStartYearUDF(df("Period")).as("collected_from_year"),
      df("Annual percentage change").as("annual_percentage_change"),
      getTrendUDF(df("Annual percentage change")).as("trend")).show(false)

    spark.stop()
  }

  def getEnglishName: (String => String) = { s => s.split(" ")(0) }

  def getStartYear: (String => String) = { s => s.split("-")(0).replace("(", "") }

  def getTrend: (Double => String) = { x => {
    if (x < -3.00) {
      "strong decline"
    }
    else if (-3.00 <= x && x <= -0.50) {
      "weak decline"
    }
    else if (-0.50 < x && x < 0.50) {
      "no decline"
    }
    else if (0.50 <= x && x <= 3.00) {
      "weak increase"
    }
    else if (x > 3.00) {
      "strong increase"
    }
    else {
      ""
    }
  }
  }
}


