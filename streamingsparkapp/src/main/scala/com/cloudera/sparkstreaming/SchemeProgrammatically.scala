package com.cloudera.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SchemeProgrammatically {
  def main(args: Array[String]){
    val master = args(0)
    val sparkconf = new SparkConf().setMaster(master).setAppName("SchemaProgrammatically")
    val sc = new SparkContext(sparkconf)
    val sqlCtx = new SQLContext(sc)
    
    val people = sc.textFile("people.txt")
    val schemaString = "Name Age"
    
    import org.apache.spark.sql.Row
    
    import org.apache.spark.sql.types.{StructType,StructField,StringType}
    
    val schema = StructType{schemaString.split(" ").map(fieldName => StructField(fieldName,StringType,true))}
    
    val rowRdd = people.map(_.split(",")).map(p => Row(p(0),p(1).trim().toInt))
    val peopleDataFrame = sqlCtx.createDataFrame(rowRdd, schema)
    
    peopleDataFrame.registerTempTable("people")
    
    val results = sqlCtx.sql("select Name from people")
    results.map(t => "Name :"+t(0)).collect().foreach(println)
    
    // print by field
    results.map(t => "Name:"+t.getAs[String]("Name")).collect().foreach(println)
    
    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    results.map(_.getValuesMap[Any](List ("Name","Age"))).collect().foreach(println)    
  }
}