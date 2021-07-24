package com.cloudera.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

object moadJhTransform {

  def main(args: Array[String]): Unit = {

    val sourceFile = args(0)
    val targetLocation = args(1) //ie latest

    println("\n*******************************")
    println("\n*******************************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("\n**********INPUTS***************")
    println("Source File: " + sourceFile)
    println("Target File Location: " + targetLocation)
    println("\n*******************************")
    println("\n*******************************")

    val spark = SparkSession.builder
      .appName("Transform John Hopkins to Parquet")
      .getOrCreate()

    val df = spark.read.json(sourceFile)


    df.printSchema()


    df.select("Country","Province","Date","Type","Count","Difference","Source", "Country Latest", "Latitude", "Longitude")
      .na.drop(Seq("Country"))
      .withColumnRenamed("Date","epoch_date")
      .withColumnRenamed("Count","type_count")
      .withColumnRenamed("Country Latest","Country_Latest")
      .write.mode(SaveMode.Append).parquet(targetLocation)
  }
}
