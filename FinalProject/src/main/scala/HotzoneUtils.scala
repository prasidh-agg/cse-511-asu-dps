object HotzoneUtils {
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    if (queryRectangle == null || queryRectangle.isEmpty || pointString == null || pointString.isEmpty)
      return false

    val rectangleArray = queryRectangle.split(",")
    val x1 = rectangleArray(0).toDouble
    val y1 = rectangleArray(1).toDouble
    val x2 = rectangleArray(2).toDouble
    val y2 = rectangleArray(3).toDouble

    val pointArray = pointString.split(",")
    val x = pointArray(0).toDouble
    val y = pointArray(1).toDouble

    if (x >= x1 && x <= x2 && y >= y1 && y <= y2)
      true
    else if (x >= x2 && x <= x1 && y >= y2 && y <= y1)
      true
    else
      false
  }
}







