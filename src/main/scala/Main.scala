import org.joda.time.format.DateTimeFormat
import org.joda.time.LocalTime
import org.joda.time.LocalDate
import org.apache.spark._
import scala.util.matching.Regex

object Main extends App {

 override def main(arg: Array[String]): Unit = {
   var sparkConf = new SparkConf().setMaster("local").setAppName("flight")
   var sc = new SparkContext(sparkConf)
   var firstRdd = sc.textFile("file:///home/suket/Documents/flights.csv")
   var AirlineRdd = sc.textFile("file:///home/suket/Documents/airlines.csv")

   var filterRdd = firstRdd.filter(x => !x.contains("YEAR"))
   var FinalAirlineRdd = AirlineRdd.filter(x => !x.contains("IATA_CODE"))
   var AirlineNameData = FinalAirlineRdd.map (_.split(",")).map {case Array(a,b) => (a,b)}.collect.toMap



   // val numberPattern: Regex = "[0-9A-Za-z],{2,}[0-9A-Za-z]".r
   // var finalFilterRdd = filterRdd.filter( x => numberPattern.findFirstMatchIn(x) == None)


   var finalFilterRdd = filterRdd.filter{x =>
       val numberPattern: Regex = "[0-9A-Za-z],{2,}[0-9A-Za-z]".r
  
    numberPattern.findFirstMatchIn(x) == None
} 
   var FinalRdd = finalFilterRdd
   // var FinalRdd = spark.sparkContext.parallelize(finalFilterRdd.top(10000))
   var flightData = FinalRdd.map(parse)
   var totalFlights = flightData.count().toDouble
   var DelayedFlights = flightData.filter(_.dep_dely > 0)
   var delayedFlightCount = DelayedFlights.count().toDouble
   var AverageDelay = (DelayedFlights.map(_.dep_dely.toDouble).reduce( _ + _))/delayedFlightCount
   var percentageDelayed = delayedFlightCount/totalFlights*100
   val AirlineData = flightData.map(flight => (flight.airline,flight))
   val grouped = AirlineData.groupByKey()
   val NumberOfFlightsOfAirlines =  grouped.map{ case (k, v) => (k, v.size) }.collect.toList
   // ^^^^^^^^^^^^^^^^^Less Efficient^^^^^^^^^^^^^^^^
   // val NumberOfFlightsOfAirlines =  AirlineData.countByKey
   val DelayedFlightsAirlineWise = grouped.map {case (k,v) => (k,v.filter(_.dep_dely > 0))}
   val AverageDelayAirLineWise = DelayedFlightsAirlineWise.map { case (k,v) => (k, v.size ,(v.map(_.dep_dely.toDouble).reduce(_+_))/v.size)}.collect.toList
     //  (Airline,DelayedFlightsCount,AverageDelay)
    val AirlineDatawithDelays =    AverageDelayAirLineWise.zip(NumberOfFlightsOfAirlines).map {case ((a,b,c),(e,d)) => (a,b,c,d)}
     // (Airline,DelayedFlightsCount,AverageDelay,TotalFlights)
    val FullAirlineData = AirlineDatawithDelays.map {case (a,b,c,d) => (AirlineNameData.get(a).get,b,c,d,(b.toFloat/d)*100)}.sortWith(_._5 > _._5)
    // (Airline,DelayedFlightsCount,AverageDelay,TotalFlights,PercentageDelay)
  

   // var totalDistanceOfAllAirlines = flightData.map(_.distance).reduce((x, y) => x + y)
   // var DistanceAirlineWise = grouped.map{ case (k,v) => (k,v.map(_.distance).reduce(_ + _))}.collect.toList


   println(f"total Flights = $totalFlights%.0f")
   println(f"Delayed Flights = $delayedFlightCount%.0f")
   println(f"Percentage of Delayed Flights = $percentageDelayed%4.2f "+"%")
   println(f"Average Delay Time = $AverageDelay%4.3f ")

   // println("Number Of Flights Of Airlines = "+ NumberOfFlightsOfAirlines)
   println()
   for ( count <- 0 to FullAirlineData.length-1){
    var tuple = FullAirlineData(count)
    var airline = tuple._1
    var delayedFlights = tuple._2
    var averageDelay = tuple._3
    var totalFlights = tuple._4
    var percDelay = tuple._5
    println(f"Airline = $airline%30s  Total Flights = $totalFlights%7.0f  Delayed Flights = $delayedFlights%7.0f  Percentage Delay = $percDelay%4.2f"+" %"+f"  Average Delay = $averageDelay%4.2f")
   }
   // println("totalDistanceOfAllAirlines = "+totalDistanceOfAllAirlines)
   // println("DistanceAirlineWise = "+ DistanceAirlineWise)
   sc.stop()
   
 }

 case class Flight(date: LocalDate,airline: String,flightNumber: String,origin: String,dest: String, dep: LocalTime, dep_dely: Double, arv: LocalTime,ar_delay: Double, airtime: Double, distance: Double) extends Serializable with Ordered[Flight]{
   def compare(that:Flight) = date.toString().compare(that.date.toString())

 }

  def parse(row: String): Flight = {
   val fields = row.split(",")
   if(fields(10)=="2400") fields(10) = "2359"
   if(fields(21)=="2400") fields(21) = "2359"
   val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
   val timepattern = DateTimeFormat.forPattern("HHmm")
   val date: LocalDate = datePattern.parseDateTime(fields(0)+"-"+fields(1)+"-"+fields(2)).toLocalDate()
   val airline: String = fields(4)
   val flightNumber: String = fields(5)
   val origin: String = fields(7) 
   val dest: String = fields(8)
   val dep: LocalTime = timepattern.parseLocalTime(fields(10))
   val dep_dely: Double = fields(11).toDouble
   val arv: LocalTime = timepattern.parseLocalTime(fields(21))
   val ar_delay: Double = fields(22).toDouble
   val airtime: Double = fields(16).toDouble
   val distance: Double = fields(17).toDouble
   
   Flight(date, airline, flightNumber, origin, dest, dep, dep_dely, arv, ar_delay, airtime, distance)

 }
}






// import scala.util.matching.Regex
// import org.joda.time.format.DateTimeFormat
// import org.joda.time.LocalTime
// import org.joda.time.LocalDate
// import org.apache.spark._
// var firstRdd = sc.textFile("file:///home/suket/Documents/flights.csv")
// var filterRdd = firstRdd.filter(x => !x.contains("YEAR"))
// var data = filterRdd.top(1)

// var fields = data(0).split(",")
// val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
//    val timepattern = DateTimeFormat.forPattern("HHmm")
//    val date: LocalDate = datePattern.parseDateTime(fields(0)+"-"+fields(1)+"-"+fields(2)).toLocalDate()
//    val airline: String = fields(4)
//    val flightNumber: String = fields(5)
//    val origin: String = fields(7)
//    val dest: String = fields(8)
//    val dep: LocalTime = timepattern.parseLocalTime(fields(10))
//    val dep_dely: Double = fields(11).toDouble
//    val arv: LocalTime = timepattern.parseLocalTime(fields(21))
//    val ar_delay: Double = fields(22).toDouble
//    val airtime: Double = fields(16).toDouble
//    val distance: Double = fields(17).toDouble







// val result = sc.parallelize(data).map { case (key, value) => (key, (value, 1)) }.reduceByKey { case ((value1, count1), (value2, count2))=> (value1 + value2, count1 + count2)}.mapValues {case (value, count) =>  value.toDouble / count.toDouble}

 // NumberOfFlightsOfAirlines.zip(DelayedFlightsAirlineWiseCount)