package main.scala.SecondHighestSalaryUsingWindowFunc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SecondHighestSalaryUsingWindowFunc extends App{
val spark = SparkSession.builder.appName("SecondHighestSalaryUsingWindowFunc").master("local").getOrCreate()
  val df_read = spark.read.format("csv").option("header","true").option("inferschema","true").load("path")
  val WindowSpec = Window.partitionBy("DeptNo").orderBy(col("Salary").desc)
  val df1 = df_read.select("EmpId","EmpName","Salary","DeptNo")
  val df2 = df1.withColumn("Rank", dense_rank() over (WindowSpec))
  val df3 = df2.filter(col("Rank") === 2)
}