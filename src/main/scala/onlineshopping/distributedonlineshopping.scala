package onlineshopping

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import onlineshopping.ShortestPaths

import scala.collection.mutable.ListBuffer

object distributedonlineshopping {
  val errorRate:Int = 20

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("DistributedOnlineShopping")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", true)
      .load("./src/main/resources/newdata.csv")

    /*    val rdd = df.rdd

    rdd.take(10).foreach(println)*/

    val vertexArray = Array(
      (1L, ("dist", 28, 20)),
      (2L, ("cli1", 27, 18)),
      (3L, ("cli2", 22, 21)),
      (4L, ("cli3", 21, 32)),
    )
    val edgeArray = Array(
      Edge(1L, 2L, 3.0),
      Edge(1L, 3L, 5.0),
      Edge(1L, 4L, 11.0),

      Edge(2L, 1L, 5.0),
      Edge(3L, 1L, 7.0),
      Edge(4L, 1L, 11.0),

      Edge(2L, 3L, 6.0),
      Edge(3L, 2L, 6.0),


      Edge(2L, 4L, 6.0),
      Edge(4L, 2L, 5.0),

      Edge(3L, 4L, 16.0),
      Edge(4L, 3L, 19.0),

    )
    /*val columns = Seq("Moment", "number")
    // Let's create the vertex RDD.
  val vertices : RDD[(VertexId, String)] = df
  .select(columns.map(c => col(c)): _*)
  .distinct // we remove duplicates
  .rdd.map(_.getAs[String](0)) // transform to RDD
  .zipWithIndex // associate a long index to each vertex
  .map(_.swap)*/

    val vertexRDD: RDD[(Long, (String, Int, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArray)

    val graph: Graph[(String, Int, Int), Double] = Graph(vertexRDD, edgeRDD)

    //graph.edges.foreach(println)

    println(getShortestPath(getSourceVertex(1L, graph), getSourceVertex(2L, graph), graph))
    println(isOnTheWay(getSourceVertex(1L, graph), getSourceVertex(2L, graph), getSourceVertex(3L, graph), graph))
    getOnTheWay(getSourceVertex(1L, graph), getSourceVertex(2L, graph),graph, vertexArray)
    getOnTheWayBack(getSourceVertex(1L, graph), getSourceVertex(2L, graph),graph, vertexArray)
    /*
        val sourceVertex=graph.vertices.filter { case (id,(_,_,_)) => id == 1L}.first
        val sourceVertex2=graph.vertices.filter { case (id,(_,_,_)) => id == 2L}.first
        val dist=sourceVertex._1
        val cli1=sourceVertex2._1
        println("----------")
        println(dist)
        val result = ShortestPaths.run(graph, Seq(dist))
        result.edges.foreach(println)
        result.vertices.foreach(println)
    */

    /*    val shortestPath = result               // result is a graph
      .vertices                             // we get the vertices RDD
      .filter({case(vId, _) => vId == dist})  // we filter to get only the shortest path from v1
      .first                                // there's only one value
      ._2                                   // the result is a tuple (v1, Map)
      .get(cli1)*/

  }

  def getSourceVertex(vid: Long, graph: Graph[(String, Int, Int), Double]): VertexId = {
    val sourceVertex = graph.vertices.filter { case (id, (_, _, _)) => id == vid }.first
    val res = sourceVertex._1
    return res
  }

  def getShortestPath(vertexId1: VertexId, vertexId2: VertexId, graph: Graph[(String, Int, Int), Double]): Option[Double] = {
    val result = ShortestPaths.run(graph, Seq(vertexId2))
    //println(result.vertices.collect.mkString("\n"))
    val shortestPath = result // result is a graph
      .vertices // we get the vertices RDD
      .filter({ case (vId, _) => vId == vertexId1 }) // we filter to get only the shortest path from v1
      .first // there's only one value
      ._2 // the result is a tuple (v1, Map)
      .get(vertexId2) // we get its shortest path to v2 as an Option object
    return shortestPath
  }

  def dijkstra[VD](g: Graph[(String, Int, Int), Int], origin: VertexId) = {
    var g2 = g.mapVertices(
      (vid, vd) => (false, if (vid == origin) 0 else Double.MaxValue))

    for (i <- 1L to g.vertices.count - 1) {
      val currentVertexId =
        g2.vertices.filter(!_._2._1)
          .fold((0L, (false, Double.MaxValue)))((a, b) =>
            if (a._2._2 < b._2._2) a else b)
          ._1

      val newDistances = g2.aggregateMessages[Double](
        ctx => if (ctx.srcId == currentVertexId)
          ctx.sendToDst(ctx.srcAttr._2 + ctx.attr),
        (a, b) => math.min(a, b))

      g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) =>
        (vd._1 || vid == currentVertexId,
          math.min(vd._2, newSum.getOrElse(Double.MaxValue))))
    }

    g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
      (vd, dist.getOrElse((false, Double.MaxValue))._2))
  }
  //Compares if vertexId2 is on the way of the vertexId1 and vertexId3 according to the errorRate
  def isOnTheWay(vertexId1: VertexId, vertexId2: VertexId, vertexId3: VertexId, graph: Graph[(String, Int, Int), Double]): Boolean = {
    val total = getShortestPath(vertexId1, vertexId3, graph).get
    val way1 = getShortestPath(vertexId1, vertexId2, graph).get
    val way2 = getShortestPath(vertexId2, vertexId3, graph).get
    if(way1 + way2 - errorRate <= total) {
      return true
    }
    return false
  }
  var visited = new ListBuffer[Long]()

  def getOnTheWay(vertexId1: VertexId, vertexId2: VertexId, graph: Graph[(String, Int, Int), Double], vertexArray: Array[(Long, (String, Int, Int))]): Unit = {
    for(item <- vertexArray if item._1 != vertexId1 && item._1 != vertexId2) {
      if(isOnTheWay(vertexId1, item._1, vertexId2, graph)) {
        visited += item._1
        println(item._1 + "is on the way");
      }
    }
  }
  def getOnTheWayBack(vertexId1: VertexId, vertexId2: VertexId, graph: Graph[(String, Int, Int), Double], vertexArray: Array[(Long, (String, Int, Int))]): Unit = {
    for(item <- vertexArray if item._1 != vertexId1 && item._1 != vertexId2) {
      if(isOnTheWay(vertexId2, item._1, vertexId1, graph)) {
        if(!visited.exists(i => i == item._1)){
          println(item._1 + "is on the return way");

        }
      }
    }
  }
}

