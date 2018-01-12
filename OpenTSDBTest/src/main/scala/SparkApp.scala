import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import breeze.linalg._
import breeze.stats._
import java.io._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scalaj.http.Http

object SparkApp {
	

  
  def phase1calcs(phase1data: breeze.linalg.DenseMatrix[Double]) = {
	var isOCsignal = 0
	var CP = 0
	var U1 = phase1data
	//Phase I calculations
	val n = U1.cols
	val n1 = U1.rows

	val Cov = breeze.linalg.cov(U1)
	val SingValDec = breeze.linalg.svd(Cov)
	val A = SingValDec.U.t
	var stdData = breeze.numerics.sqrt(breeze.linalg.diag(breeze.numerics.abs(A * Cov * A.t)))
	
	val result = (A, stdData)	
	result;
  }
  
  def phase2calcs(U2p: breeze.linalg.DenseMatrix[Double], stdDatap: breeze.linalg.DenseVector[Double], Ap: breeze.linalg.DenseMatrix[Double], alphap: Double, phase1point:Int = 0) = {
	val n2 = U2p.rows
	val n = U2p.cols
	
	
	val alpha = alphap
	var ocList = ListBuffer[Integer]()
	var diffList = ListBuffer[Integer]()
	val U2 = U2p
	val stdData = stdDatap
	val A = Ap
	var CP = 0
	//begin looking for anomalies
	var sumDiffs = 0
	var consecDiffs = 0
	var i = 0
	var doneWithAll = 0
	while(i < n2 && doneWithAll != 1){
	  var X = U2.t(::,i)
	  var Y = A * X
	  var Ystandard = Y :/ stdData   //might need to use 1 instead of 0? not sure about the axis right now
	  
	 val norm = new breeze.stats.distributions.Gaussian(0,1)
	 var Pvalue = new ListBuffer[Double]()
	 for(k <- Ystandard){
	   Pvalue.append(norm.cdf(k))
	 }
	 var PVsort = Pvalue.toList.sorted

	 //inner loop
	 var isOCsignal = 0
	 var done = 0
	 var j = 0
	 while(j < n-1 && done != 1){
	   if(PVsort(j) < (j+1)*alpha/n){
		 isOCsignal = 1
		 if(PVsort(j+1) > (j+2)*alpha/n){
		   done = 1
		 }
	   } 
	   j += 1;
	 }

	 if(isOCsignal ==1){
	   CP = i + phase1point
	   ocList.append(CP)
	   consecDiffs += 1
	 }
	 else{
	   consecDiffs = 0
	 }
	 if(consecDiffs == 5){
	  // doneWithAll = 1
	 }

	 i += 1;

	}
	val result = (ocList mkString "\n")
	result;
  } 

  def main(args: Array[String]){
    //constants
    val phase1point = 900
    val alpha = 0.005

    //setting up data
    //val filename = args(0)
    //var data = breeze.linalg.csvread(new java.io.File(filename), ' ')  //this is for running not on hdfs
    
    //val request = scalaj.http.Http("http://mimosa40.cc.gatech.edu:4242/api/query?start=1477958400&end=1477959400&m=max:1m-max-none:energy.salted").asString
    //val request=scalaj.http.Http("http://mimosa40.cc.gatech.edu:4242/api/query?start=1477958400&end=1477987200&m=sum:energy.salted{unit=0,sensor=0}").asString

    //println(request)
  
    //val request1=scalaj.http.Http("http://mimosa40.cc.gatech.edu:4242/api/query?start=1477958400&end=1477987200&m=sum:energy.salted{unit=0}").asString
    //println(request1)
  
    val request2=scalaj.http.Http("http://mimosa40.cc.gatech.edu:4242/api/query?start=1477958400&end=1477987200&m=avg:energy.salted{unit=0}").asString
    println(request2)

    /*var U1 = data(0 until phase1point, ::)
    var U2 = data(phase1point to -1, ::)
	
    val phase1results = phase1calcs(U1)
    val A = phase1results._1
    val stdData = phase1results._2
    val anomalies = phase2calcs(U2, stdData, A, alpha, phase1point)
    println(anomalies)*/
  }
}
	
    //this segment is for running on hdfs.  It doesn't work yet.  
    //val conf = new SparkConf()
    //val sc = new SparkContext(conf)
    //val rddData = sc.textFile(filename).map(line => Vectors.dense(line.split(' ').map(_.toDouble))) 
    //var data = breeze.linalg.DenseMatrix(rddData.collect())
    //end segment


