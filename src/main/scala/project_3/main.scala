package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var g = g_in
    var remaining_vertices = 2 // kick off the while loop
    val r = scala.util.Random
    var iterCount = 0

    while (remaining_vertices >= 1) {
    
      // assign random values to all vertices that are active
      val newGraph: Graph[Float, Int] = g.mapVertices((id, attr) => if (attr == 0) r.nextFloat else attr )

      // send values to neighbors, we only care about the largest value so we can compare to newGraph's random vals
      val msg: VertexRDD[Float] = newGraph.aggregateMessages[Float](//random values will be floats
        triplet => {
          if (triplet.srcAttr != 0) {
            triplet.sendToDst(triplet.srcAttr)
          } else { triplet.sendToSrc(triplet.srcAttr)}
          if (triplet.dstAttr != 0) {
            triplet.sendToSrc(triplet.dstAttr)
          } else { triplet.sendToDst(triplet.dstAttr)}
        }, (a, b) => {
          if (a > b) a else b
        }
      )

      
      //  if bv (oldAttr) > bx for every neighbor (newAttr is max of these anyways) add to MIS (+1)
      val joinedGraph: Graph[Float, Int] = newGraph.joinVertices(msg) { (_, oldAttr, newAttr) =>
        if (newAttr < oldAttr) 1 else 0 } 

       
      // send message if added to MIS (ie is a +1)
      val msg2: VertexRDD[Float] = joinedGraph.aggregateMessages[Float](
        triplet => {
          if (triplet.srcAttr == 1) { triplet.sendToDst(-1); triplet.sendToSrc(1)
          } else { triplet.sendToDst(0) }
          if (triplet.dstAttr == 1) { triplet.sendToSrc(-1); triplet.sendToDst(1)
          } else { triplet.sendToSrc(0) }
        }, (a, b) => (a + b))


      // adjust so all positives return to +1 and negatives return to -1 
      val joinedGraph2: Graph[Float, Int] = joinedGraph.joinVertices(msg2) { (_, oldAttr, newAttr) =>
        if (newAttr != 0) newAttr/(newAttr.abs) else 0
      }

      // typecast and find remaining 0's
      remaining_vertices = joinedGraph2.vertices.filter { case (id, attr) => attr == 0 }.count.asInstanceOf[Int]
      println("Remaining vertices:" + remaining_vertices)
      val typecastedGraph: Graph[Int, Int] = joinedGraph2.mapVertices((id, attr) => attr.asInstanceOf[Int])

      iterCount = iterCount + 1
      g = typecastedGraph
    }
    println("Construction finished with " + iterCount + " iterations")
    

    return g
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    var g = g_in

    // check for independence -- make sure for all +1's that neighbors are <1
    val independence_vertices = g.aggregateMessages[Int](
      triplet => { 
        triplet.sendToDst(triplet.srcAttr)
        triplet.sendToSrc(triplet.dstAttr)
        
        // fix overcounting
        if(triplet.srcAttr == -1) { triplet.sendToSrc(-1) } 
        if(triplet.dstAttr == -1) { triplet.sendToDst(-1) }
      }, (a, b) => (a + b))

    // if a vertex with +1 recieved a +1 message, this is not a MIS
    if (independence_vertices.filter { case (a, b) => b > 0 }.count > 0) {
      return false
    }

    // check for maximallity
    val maximality_vertices = g.aggregateMessages[Int](
      triplet => {
        if(triplet.srcAttr == 1) { triplet.sendToDst(1)
        } else { triplet.sendToDst(0) }
        if(triplet.dstAttr == 1) { triplet.sendToSrc(1)
        } else { triplet.sendToSrc(0) }
      }, (a, b) => (a + b))

    if (maximality_vertices.filter { case (a, b) => b < 0 }.count > 0) {
      return false
    }

    return true
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)
      // verify after constructing
      val isMIS = verifyMIS(g)
      if(isMIS)
        println("Passed verifyMIS() check")
      else
        println("Did not pass verifyMIS() check")

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} ).filter({case Edge(a,b,c)=>a!=b})
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
