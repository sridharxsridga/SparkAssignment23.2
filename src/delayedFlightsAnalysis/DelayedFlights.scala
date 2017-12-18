/*
 * This application is to perform analysis on aviation dataset
 * 
 * 
 */


package delayedFlightsAnalysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object DelayedFlights {
  def main(args: Array[String]): Unit = {
    
    /* With Spark 2.0 , we can  SparkSession is used create datasets from files, which bundles sparkContext and sparkConf
     * Creating a sparkSession and setting master to local
     */
    
    val spark = SparkSession.builder().appName("Analsis of delayed Flights").master("local").getOrCreate()

    /* creating a dataset by reading csv file using databricks package which provides better options for handling headers
     *    
     */
    val MyDataSet = spark.read.format("com.databricks.spark.csv").option("header", "true").load("file:/home/acadgild/Final_revision/DelayedFlights.csv");

    /*
     * Creating a temporary view from the dataframe which can be used to query through sql
     */
    MyDataSet.createOrReplaceTempView("FlightData")

    
    /* Problem Statement 1 :
     * Find out the top 5 most visited destinations. 
     */
    val max5Dest = spark.sql("select dest , count(dest) as TopDestinations from FlightData group By dest order by dest desc limit 5 ")

    max5Dest.show

    /* Problem Statement 2 :
     * Which month has seen the most number of cancellations due to bad weather? 
     */
    val maxCancellationMonth = spark.sql("select Month , count(Month) as NumOfCancellations , Cancelled , CancellationCode  from FlightData  group By Month , Cancelled ,CancellationCode  having Cancelled ='Y' and CancellationCode = 'B' order by NumOfCancellations desc limit 1 ")
    
    maxCancellationMonth.show
    
    
    /*
     * Problem Statement 3
     * Top ten origins with the highest AVG departure delay
     */
    
    val originsWithHighestAvgDepDelay = spark.sql("select Origin , avg(DepDelay) as AvgDepartureDelay from FlightData  group By Origin order by AvgDepartureDelay desc limit 10 ")
    
    originsWithHighestAvgDepDelay.show
    
    
     /* Problem Statement 4
      * Which route (origin & destination) has seen the maximum diversion?
      */
   
    val maxDiversion = spark.sql("select Origin, Dest, count(Diverted) as NumberOfDiversion from FlightData group by Origin,Dest  order by NumberOfDiversion desc limit 1 ")
    
    maxDiversion.show

  
  }
}