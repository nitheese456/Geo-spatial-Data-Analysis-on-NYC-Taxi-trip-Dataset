package cse511

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    var rectangle = new Array[String](4)
    rectangle = queryRectangle.split(",")
    val x_coordinate_1 = rectangle(0).trim.toDouble
    val y_coordinate_1 = rectangle(1).trim.toDouble
    val x_coordinate_2 = rectangle(2).trim.toDouble
    val y_coordinate_2 = rectangle(3).trim.toDouble

    var point = new Array[String](2)
    point = pointString.split(",")
    val x_coordinate = point(0).trim.toDouble
    val y_coordinate = point(1).trim.toDouble

    val x_min = math.min(x_coordinate_1, x_coordinate_2)
    val x_max = math.max(x_coordinate_1, x_coordinate_2)
    val y_min = math.min(y_coordinate_1, y_coordinate_2)
    val y_max = math.max(y_coordinate_1, y_coordinate_2)

    return !(x_coordinate < x_min || x_coordinate > x_max || y_coordinate < y_min || y_coordinate > y_max)

  }
}
