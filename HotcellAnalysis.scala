package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

    def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
    {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    pickupInfo.createOrReplaceTempView("pickup_info")

    spark.udf.register("square", (input: Int) => HotcellUtils.square(input))
    spark.udf.register("getNeighboursNumber", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int) => HotcellUtils.getNeighboursNumber(minX, minY, minZ, maxX, maxY, maxZ, inputX, inputY, inputZ))
    spark.udf.register("getGetisOrdScore", (sumOfCount:Int,  numOfNeighbors:Int, mean: Double, std:Double, totalNumberOfCells: Int) => HotcellUtils.getGetisOrdScore(sumOfCount,  numOfNeighbors, mean, std, totalNumberOfCells))

    val pointsInRange = spark.sql(s"SELECT x, y, z FROM pickup_info WHERE x >= $minX AND y >= $minY AND z >= $minZ AND x <= $maxX AND y <= $maxY AND z <= $maxZ").persist()
    pointsInRange.createOrReplaceTempView("points_in_range")
    pointsInRange.show()

    // Get the points and the number of values for each set
    val pointsAndCount = spark.sql("SELECT x, y, z, COUNT(*) AS point_count FROM points_in_range GROUP BY x, y, z").persist()
    pointsAndCount.createOrReplaceTempView("points_and_count")
    pointsAndCount.show()

    // Calculate the sum and the sum of the Squares of the points.
    val sumOfPoints = spark.sql("SELECT SUM(point_count) AS sum_of_points, SUM(square(point_count)) AS squared_sum FROM points_and_count").persist()
    sumOfPoints.createOrReplaceTempView("sum_of_points")
    sumOfPoints.show()

    val sumOfPointsVal = sumOfPoints.first().getLong(0)
    val squaredSum = sumOfPoints.first().getDouble(1)

    val mean = sumOfPointsVal.toDouble / numCells.toDouble
    println(mean)

    val standardDeviation = math.sqrt((squaredSum.toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))
    println(standardDeviation)

    val neighbours = spark.sql(
      "SELECT getNeighboursNumber("+ minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "a1.x, a1.y, a1.z) AS num_of_neighbours," +
        "a1.x AS x, a1.y AS y, a1.z AS z, " + 
        "SUM(a2.point_count) AS sum_of_count " +
        "FROM points_and_count AS a1, points_and_count AS a2 " +
        "WHERE " +
            "(ABS(a2.x - a1.x) < 2) " +
            "AND (ABS(a2.y - a1.y) < 2 ) " +
            "AND (ABS(a2.z - a1.z) < 2 ) " +
                "GROUP BY a1.x, a1.y, a1.z ORDER BY a1.x, a1.y, a1.z").persist()
    neighbours.createOrReplaceTempView("neighbours_count")
    neighbours.show()

    val gScore = spark.sql(s"SELECT x,y,z, getGetisOrdScore(sum_of_count, num_of_neighbours, $mean, $standardDeviation,  $numCells) AS gScore FROM neighbours_count ORDER BY gScore DESC")
    gScore.createOrReplaceTempView("g_score")
    gScore.show()

    val finalResult = spark.sql("SELECT x,y,z from g_score").limit(50)
    finalResult.createOrReplaceGlobalTempView("result")
    finalResult.show()
    return finalResult
  }
}
