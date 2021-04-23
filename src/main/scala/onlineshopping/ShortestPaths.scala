package onlineshopping

import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}

object ShortestPaths extends Serializable {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Double]
  // initial and infinity values, use to relax edges
  private val INITIAL = 0.0
  private val INFINITY = Int.MaxValue.toDouble

  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap, delta: Double): SPMap = {
    spmap.map { case (v, d) => v -> (d + delta) }
  }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, INFINITY), spmap2.getOrElse(k, INFINITY))
    }.toMap
  }

  // at this point it does not really matter what vertex type is
  def run[VD](graph: Graph[VD, Double], landmarks: Seq[VertexId]): Graph[SPMap, Double] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      // initial value for itself is 0.0 as Double
      if (landmarks.contains(vid)) makeMap(vid -> INITIAL) else makeMap()
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, Double]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr, edge.attr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }
}
