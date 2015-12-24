package com.cloudera.sparkstreaming

import scala.collection.Map
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

object Recommender {
  def main(args: Array[String]) : Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Recommender"))
    
    val userArtistData1 = sc.textFile("sparkFiles/Input/userArtistData/user_artist_data_min.txt")
    val userArtistData = userArtistData1.filter(line => line != "")
    val rawArtistData = sc.textFile("sparkFiles/Input/userArtistData/artist_data.txt")
    val rawArtistAlias = sc.textFile("sparkFiles/Input/userArtistData/artist_alias.txt")    
    
  }
  
  def buildArtistById(rawArtistData : RDD[String]) =
    rawArtistData.flatMap { line => val (id,name) = line.span(_ != '\t')
      if(name.isEmpty){
        None
      }else{
        try{
          Some(id.toInt,name.trim)  
        }catch {
          case e: NumberFormatException => None
        }
      }
    }

  def buildArtistAlias(rawArtistAlias: RDD[String]) =
    rawArtistAlias.flatMap{
    line => val tokens = line.split(' ')
    if(tokens(0).isEmpty){
      None
    }else{
      Some(tokens(0).toInt,tokens(1).toInt)
    }
  }.collectAsMap()
  
  def buildRatings(rawUserArtistData: RDD[String],bArtistsAlias:Broadcast[Map[Int,Int]]) = 
    rawUserArtistData.map{
      line => val Array(user,artist,count) = line.split('\t').map(_.toInt)
      val finalArtistId = bArtistsAlias.value.getOrElse(artist,artist)
      Rating(user,finalArtistId,count)
    }
  
  def model(sc: SparkContext, rawUserArtistData: RDD[String],rawArtistData : RDD[String], rawArtistAlias : RDD[String]): Unit = {
    
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val trainData = buildRatings(rawUserArtistData,bArtistAlias).cache()
    
    val model = ALS.trainImplicit(trainData,10,5,0.01,1.0)
    
    trainData.unpersist()
    println(model.userFeatures.mapValues(_.mkString(", ")).first())
    
    val userId = 1043789
    val recommendations = model.recommendProducts(userId, 5)
    recommendations.foreach (println)
    val recommendedProducts = recommendations.map(_.product).toSet
    
    val rawArtistsUser = rawUserArtistData.map(_.split(' ')).filter{case Array(artist,_,_) => artist == userId}
    val existingProducts = rawArtistsUser.map{case Array(_,product,_) => product.toInt}.collect().toSet
    
    val artistById = buildArtistById(rawArtistData)
    artistById.filter{case (id,name) => existingProducts.contains(id)}.values.collect().foreach(println)
    artistById.filter{case (id,name) => recommendedProducts.contains(id)}.values.collect().foreach(println)
    
    unpersists(model)
  }
  
  
  
  def evaluate(sc: SparkContext, rawUserArtistData: RDD[String], rawArtistAlias: RDD[String]): Unit = {
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData,bArtistAlias)
    val Array(trainData,cvData) = allData.randomSplit(Array(0.9, 0.1))
    
    trainData.cache()
    cvData.cache()
    
    val allItemIds = allData.map(_.product).distinct().collect()
    val bAllItemIds = sc.broadcast(allItemIds)
    
    val mostListenedAuc = areaUnderCurve(cvData,bAllItemIds,predictMostListened(sc,trainData)) 
    println(mostListenedAuc)
    
    val evaluations = for(rank <- Array(10,  50);
    lambda <- Array(1.0, 0.0001);
    alpha  <- Array(1.0, 40.0))
      yield{
        val model = ALS.trainImplicit(trainData, rank, 10,lambda,alpha)
        val auc = areaUnderCurve(cvData,bAllItemIds,model.predict)       
         unpersists(model)
        ((rank,lambda,alpha),auc)
    }
    evaluations.sortBy(_._2).reverse.foreach(println)
    
    trainData.unpersist()
    cvData.unpersist()
  }
  
  def predictMostListened(sc: SparkContext,train: RDD[Rating])(allData : RDD[(Int,Int)]) = {
    val bListenCount = sc.broadcast(train.map(r => (r.product, r.rating)).reduceByKey(_+_).collectAsMap())
    allData.map{case (user,product) => Rating(user,product,bListenCount.value.getOrElse(product,0.0))}
   // allData.map( r => (r._1,r._2,bListenCount.value.getOrElse(r._2,0.0)))
  }
  
  def areaUnderCurve(positiveData:RDD[Rating],bAllItemIds:Broadcast[Array[Int]],predictFunction: RDD[(Int,Int)] => RDD[Rating]){
    val positiveUserProducts = positiveData.map(r => (r.user,r.product))
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)
    
    val negativeUserProducts = positiveUserProducts.groupByKey.mapPartitions{
      userIdsAndPosItemIds => {
        val random = new Random()
        val allItemIds = bAllItemIds.value
        userIdsAndPosItemIds.map{
          case (user,product) => {
            val negative = new ArrayBuffer[Int]()
            val posItemIdSet = product.toSet
            var i = 0
            
            while(i < allItemIds.size && negative.size < posItemIdSet.size){
              val itemId = allItemIds(random.nextInt(allItemIds.size))
              if(!posItemIdSet.contains(itemId)){
                negative += itemId
              }
              i += 1
            }
            negative.map (itemId => (user,itemId))
          }
        }
      }
    }.flatMap(t => t)
    
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)
    
    positivePredictions.join(negativePredictions).values.map{
      case (positiveRatings,negativeRatings) =>
        var count = 0L
        var total = 0L
        for(positive <- positiveRatings;
            negative <- negativeRatings){
          if(positive.rating > negative.rating){
            count += 1
          }
          total += 1  
        }
        count.toDouble/total
    }.mean()
  }
    
  def recommend(sc: SparkContext, rawUserArtistData: RDD[String], rawArtistData: RDD[String], rawArtistAlias: RDD[String]) : Unit = {
    val bArtistAlias = sc.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildRatings(rawUserArtistData,bArtistAlias).cache()
    val model = ALS.trainImplicit(allData,50,10,1.0,40.0)
    allData.unpersist()
    
    val userId = 1043789
    
    val recommendations = model.recommendProducts(userId, 5)
    val recommendedProducts = recommendations.map(_.product).toSet
    
    val artistById = buildArtistById(rawArtistData)
    artistById.filter{case (id,name) => recommendedProducts.contains(id)}.values.collect().foreach(println)
    
    val someUsers = allData.map(_.user).distinct().take(100)
    val someRecommendations = someUsers.map(user => model.recommendProducts(user,5))
    someRecommendations.map(recs => recs.head.user + " -> " + recs.map(_.product).mkString(",")).foreach(println)
    
    unpersists(model)
  }
  
   def unpersists(model: MatrixFactorizationModel): Unit = {
     model.userFeatures.unpersist()
     model.productFeatures.unpersist()
   }   
}
