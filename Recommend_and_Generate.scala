import org.apache.spark.mllib.recommendation.ALS
import org.jblas.DoubleMatrix
import java.io._




val circleRawData = sc.textFile("/Users/YCheng/Desktop/Recommand/data.circles")
val rawFbData = sc.textFile("/Users/YCheng/Desktop/Recommand/data.feat")
val edgeData = sc.textFile("/Users/YCheng/Desktop/Recommand/data.edges")
val featRddPair =  rawFbData.map(x => (x.split(" ")(0).toInt, x.split(" ").map(_.toDouble).slice(2,225)))

val IDNum = 29
val testnum = 1
// val file = "/Users/YCheng/Desktop/Recommand/result.txt"
// file < "NodeID RecommendID Similarity"
val writefile = new PrintWriter(new File("/Users/YCheng/Desktop/Recommand/result.txt" ))
writefile.write("NodeID RecommendID Similarity\n")
writefile.close()



def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
	vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
}


def recommendSingle(nodeid: Int){
	val baseUserVector = new DoubleMatrix(featRddPair.lookup(nodeid).head)
	val sims = featRddPair.map{case(id, factor) =>
		val factorVector = new DoubleMatrix(factor)
		val sim = cosineSimilarity(factorVector, baseUserVector)
		(id, sim)
	}
	val num = 10
	val sortRecommend = sims.top(20)(Ordering.by[(Int, Double), Double]{case (id, similarity) => similarity})

	//println("\n")
	//println(sortRecommend.take(30).mkString("\n"))
	//Importing Circles
	
	val circleRddPair = circleRawData.map(x => (x.split("\t")(0), x.split("\t").slice(1,40)))
	val circles = circleRddPair.mapValues(x => x.map(y => y.toInt))

	//Importing Edges
	
	val edgePair = edgeData.map(x => (x.split(" ")(0).toInt, x.split(" ")(1).toInt))
	val sortedRecommend = sims.sortBy(_._2,false)
	val IDEdges = edgePair.lookup(nodeid).toArray
	//Find the edges including the target node
	val recommendofTarget = sortedRecommend.keys.take(20)

	val recommendFinal = (recommendofTarget.toSet -- IDEdges.toSet -- Array(nodeid).toSet).toArray

	val recommendPairs = sims.filter(x => recommendFinal.contains(x._1))
	//println(recommendPairs.mkString("\n"))
	val outputPairs = recommendPairs.map(x => (nodeid, x))
	val outputResult = outputPairs.map{case(id1,(id2, similarity)) => (id1, id2, similarity)}
	val output_sorted = outputResult.sortBy(_._3,false)
	
	val write = new PrintWriter(new FileOutputStream(new File("/Users/YCheng/Desktop/Recommand/result.txt"),true))
	//output_sorted.repartition(1).saveAsTextFile("/Users/YCheng/Desktop/result.txt")
	
	write.write(output_sorted.take(10).to.mkString("\n"))
	write.close()
	//println(output_sorted.take(10).mkString("\n"))
	
	
	
	//output
	
}

//recommendSingle(1)
for (i <- 1 to 3){
//for (i <- 1 to 5){

	recommendSingle(i)
	val write = new PrintWriter(new FileOutputStream(new File("/Users/YCheng/Desktop/Recommand/result.txt"),true))
	write.write("\n")
	write.close()
}

//println(finalOutput.mkString("\n"))


//recommendPairs.coalesce(1,true).saveAsTextFile("/Users/YCheng/Desktop/test")

System.exit(0)
		
