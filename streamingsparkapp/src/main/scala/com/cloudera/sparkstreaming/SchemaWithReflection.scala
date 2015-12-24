package com.cloudera.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SchemaWithReflection {
  def main(args: Array[String]){
    val master = args(0)
    val sparkconf = new SparkConf().setMaster(master).setAppName("StreamingWithReflection")
    val sc = new SparkContext(sparkconf)
    val sqlCtx = new org.apache.spark.sql.SQLContext(sc)
    
    import sqlCtx.implicits._
    
    case class Person(Name: String , Age: Int)
    val people = sc.textFile("people.txt").map(l => l.split(",")).map(p => Person(p(0),p(1).trim.toInt))
    
    //val peopleDataFrame = people.toDF()
    
    

    
  }
}