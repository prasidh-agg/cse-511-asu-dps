import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )
    spark.udf.register("CalculateY",(pickupPoint: String)=>
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )
    spark.udf.register("CalculateZ",(pickupTime: String)=>
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    val newCoordinateName = Seq("x", "y", "z")
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

    // YOU NEED TO CHANGE THIS PART

    //Make a temp view
    pickupInfo.createOrReplaceTempView("pickupInfoView")

    //Check if particular point is in cell boundary
    spark.udf.register("checkCellInBounds", (x: Double, y:Double, z:Int) =>  (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY) && (z >= minZ) && (z <= maxZ) )

    val filteredPoints = spark.sql("select x,y,z from pickupInfoView where checkCellInBounds(x, y, z) order by z,y,x").persist()
    filteredPoints.createOrReplaceTempView("filteredPointsView")

    val filteredPointsCount = spark.sql("select x,y,z,count(*) as numPoints from filteredPointsView group by z,y,x order by z,y,x").persist()
    filteredPointsCount.createOrReplaceTempView("filteredPointsCountView")

    spark.udf.register("square", (inputX: Int) => (inputX*inputX).toDouble)
    val sumOfPoints = spark.sql("select count(*) as numCellsWithAtleastOnePoint, sum(numPoints) as totalPointsInsideTheGivenArea, sum(square(numPoints)) as squaredSumOfAllPointsInGivenArea from filteredPointsCountView")
    sumOfPoints.createOrReplaceTempView("sumOfPoints")

    val totalPoints = sumOfPoints.first().getLong(1) //sigma xj
    val squaredSumOfCells = sumOfPoints.first().getDouble(2) //sigma (xj**2)


    val Xbar = totalPoints / numCells
    val SD = math.sqrt((squaredSumOfCells / numCells) - (Xbar * Xbar) )


    spark.udf.register("findTotalNeighbours", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, Xin: Int, Yin: Int, Zin: Int)
    => HotcellUtils.findTotalNeighbours(minX, minY, minZ, maxX, maxY, maxZ, Xin, Yin, Zin))
    val Neighbours = spark.sql("select " +
                                                "view1.x as x, " +
                                                "view1.y as y, " +
                                                "view1.z as z, " +
                                                "findTotalNeighbours("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "view1.x,view1.y,view1.z) as totalNeighbours, " +   // function to get the number of neighbours of x,y,z
                                                "count(*) as neighboursWithValidPoints, " +
                                                "sum(view2.numPoints) as sumAllNeighboursPoints " +
                                                "from filteredPointsCountView as view1, filteredPointsCountView as view2 " +
                                                "where (view2.x = view1.x+1 or view2.x = view1.x or view2.x = view1.x-1) and (view2.y = view1.y+1 or view2.y = view1.y or view2.y = view1.y-1) and (view2.z = view1.z+1 or view2.z = view1.z or view2.z = view1.z-1) " +   //join condition
                                                "group by view1.z, view1.y, view1.x order by view1.z, view1.y, view1.x").persist()

    Neighbours.createOrReplaceTempView("NeighboursView")

    spark.udf.register("calculateGScore", (x: Int, y: Int, z: Int, numCells: Int, mean:Double, sd: Double, totalNeighbours: Int, sumAllNeighboursPoints: Int) =>
      HotcellUtils.calculateGScore(x, y, z, numCells, mean, sd, totalNeighbours, sumAllNeighboursPoints))
    val NeighboursDesc = spark.sql("select x, y, z, " +
                                            "calculateGScore(x, y, z," +numCells+ ", " + Xbar + ", " + SD + ", totalNeighbours, sumAllNeighboursPoints) as gi_statistic " +
                                            "from NeighboursView " +
                                            "order by gi_statistic desc")
    NeighboursDesc.createOrReplaceTempView("NeighboursDescView")
    NeighboursDesc.show()

    val hotCellsDescendingOrder = spark.sql("select x,y,z from NeighboursDescView")

    hotCellsDescendingOrder
  }
}
