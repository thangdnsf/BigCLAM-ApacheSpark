import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import scala.io.Source
import scala.math.abs
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry,IndexedRow,IndexedRowMatrix,BlockMatrix}
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.{Vectors,Vector,Matrix,Matrices}
import scala.util.Random
import breeze.linalg.{DenseMatrix => BDM,SparseVector => BSV, Vector => BV}
import org.apache.spark.broadcast.Broadcast

var numCore = 2
var K = sc.broadcast(8385)

var F:RDD[(Long,BSV[Double])] = _

var sumF:BDM[Double] = null
val epsCommForce = 1e-6
val MIN_P_ = 0.0001
val MAX_P_ = 0.9999
val MIN_F_ = 0.0
val MAX_F_ = 1000.0

var G = GraphLoader.edgeListFile(sc, "file:///home/thangdn/Desktop/com-youtube.ungraph.txt")

//var edgeListrdd = G.ops.collectEdges(EdgeDirection.Out).map(x => x._2).flatMap(y =>y).map(x=>Edge(x.dstId,x.srcId,x.attr)).union(G.ops.collectEdges(EdgeDirection.In).map(x => x._2).flatMap(y =>y))
//var A = new CoordinateMatrix(edgeListrdd.map(x => MatrixEntry(x.srcId,x.dstId,x.attr)))

val collectNeighbor = G.ops.collectNeighborIds(EdgeDirection.Either).repartition(numCore).cache()
var Neightborbc=sc.broadcast(collectNeighbor.collectAsMap())


def getEgoGraphNodes(): RDD[(VertexId, Array[VertexId])]={
    return collectNeighbor.map(x =>(x._1,Array(x._1)++x._2))
  }

def conductanceLocalMin(): Array[Long]={
    // compute conductance for all nodes
    val egonet=getEgoGraphNodes()
    val sigmaDegres = sc.broadcast((G.inDegrees ++ G.outDegrees).reduceByKey(_+_).map(_._2).reduce(_+_))
    val ConductanceRDD =egonet.map{case(x,y)=>
      var z = y.flatMap(u => Neightborbc.value(u));
      var cut_S = z.map(i => if(y.contains(i)) 0.0 else 1.0).filter(m => m == 1.0).size;
      var vol_S = z.size-cut_S;
      var vol_T = sigmaDegres.value-vol_S-cut_S*2;
      (x,if(vol_S == 0) 0 else if(vol_T == 0) 1 else cut_S.toDouble/math.min(vol_S,vol_T))
    }
    var ConductanceRDDbc = sc.broadcast(ConductanceRDD.collectAsMap)
    var indx =collectNeighbor.map{case(x,y) => if (y.size > 0) y.map(conducu => (conducu,ConductanceRDDbc.value(conducu))).min else (x,10.0)}.reduceByKey{case(a,b) => if(a<b) a else b}.map{case(x,y)=>(y,x)}.sortByKey().map(_._2).collect
    sigmaDegres.destroy()
    return indx
  }

def randomIndexedRow(index:Long,n : Int ):IndexedRow={
    IndexedRow(index.toLong,Vectors.dense(Array.fill(n)(Random.nextInt(2).toDouble)).toSparse)
  }
  
  def initNeighborComF(): Unit={
    // Get S set which is conductance of locally minimal
    var S = conductanceLocalMin().take(K.value)
    var siG = sc.broadcast(G.vertices.count)
    var rows = collectNeighbor.filter(x => S.contains(x._1)).map{x =>
     Vectors.sparse(siG.value.toInt,x._2.map(x => x.toInt),Array.fill(x._2.size)(1.0))}.zipWithIndex.map(x => IndexedRow(x._2,x._1))
    // If S set < K set then random K - S set
    //fix it into sparse
    var addRows:Array[IndexedRow] = null
    for (i <- (S.size) to (K.value-1)) {
      if (addRows == null) {
        addRows = Array(randomIndexedRow(i, siG.value.toInt))
      }
      else {
        addRows ++= Array(randomIndexedRow(i, siG.value.toInt))
      }
    }
    if (addRows != null){
      var addRowsRDD = sc.makeRDD(addRows)
      //addRowsRDD= addRowsRDD.map{case IndexedRow(x,y) => IndexedRow(x,y.toSparse)}
      rows = rows.union(addRowsRDD)
    }
    //Create matrix F
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)
    sumF = BDM.create(1,K.value,mat.toCoordinateMatrix.entries.repartition(numCore).map{case MatrixEntry(row,_,value) => 
      (row,value)}.reduceByKey(_+_).sortByKey().map(_._2).collect)
    F = mat.toCoordinateMatrix.transpose.toIndexedRowMatrix.rows.repartition(numCore).map(x => (x.index,new BSV[Double](x.vector.toSparse.indices,x.vector.toSparse.values,K.value))).cache
  }

  def step(Fu:BDM[Double], stepSize:Double, direction:BDM[Double]):BDM[Double]= {
    BDM.create(1,K.value,(Fu + stepSize * direction).data.map{x =>
      math.min(math.max(x ,MIN_F_),MAX_F_)})
  }

  def Flookup(Fbc:Broadcast[scala.collection.Map[Long,breeze.linalg.SparseVector[Double]]],row: VertexId ):BDM[Double]={
    var frow: BDM[Double] = null;
    try{frow = Fbc.value(row).toDenseVector.toDenseMatrix}catch{ case e : NoSuchElementException => frow = BDM.create(1,K.value, Array.fill(K.value)(Random.nextInt(1).toDouble))};
    return frow
  }

  def FlookupSparseVector(Fbc:Broadcast[scala.collection.Map[Long,breeze.linalg.SparseVector[Double]]],row: VertexId ):BSV[Double]={
    var frow: BSV[Double] = null
    try{frow = Fbc.value(row)}catch{ case e : NoSuchElementException => frow = new BSV[Double](Array(),Array(),K.value)}
    return frow
  }

  def loglikelihood():Double ={
    var Fbc=sc.broadcast(F.collectAsMap())
    val result = collectNeighbor.map{case(x,y)=>
      var fu = Flookup(Fbc,x)
      val fusfT = (fu * BDM.create(K.value, 1, sumF.data)).data(0)
      val fufuT = (fu * BDM.create(K.value, 1, fu.data)).data(0)
      y.map {m =>
      var fv = Flookup(Fbc,m)
      var fvT = BDM.create(K.value, 1, fv.data);
        var fufvT = (fu * fvT).data(0);
        (math.log(1 - math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_))+ fufvT)}.reduce(_+_) - fusfT + fufuT
    }.reduce(_+_)
    Fbc.destroy()
    return result
  }
  var alpha = 0.05
  var beta = 0.1
  var MaxIter =15
  //var oldFuT = BDM.create(K.value,1,fu.data)
  var stepSize: Double = 1.0
  var listSearch: List[Double] = List(1.0)
  for(i <- 1 to MaxIter) {
    stepSize *=beta
    listSearch = stepSize::listSearch
  }
  var liststepSizeRDD = sc.makeRDD(listSearch)

  def backtrackingLineSearchs(uset:List[Long]): Double =
  {
    var Fbc=sc.broadcast(F.collectAsMap)

    //PRE-BACKTRACKING LINE SEARCH
    var result1 = collectNeighbor.filter(x=> uset.contains(x._1)).map{case(ux,y)=>
      var fu=Flookup(Fbc,ux)
      var fusfT = (fu*BDM.create(K.value,1,sumF.data)).data(0);
      var sf = sumF
      var fufuT = (fu*BDM.create(K.value,1,fu.data)).data(0);
      var kq =y.map {m =>
        var fv = Flookup(Fbc,m)
        var fvT = BDM.create(K.value, 1, fv.data);
        var fufvT = (fu * fvT).data(0);
        var fufvTrange = math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_);
        (math.log(1- fufvTrange)+ fufvT,fv*(1/(1 - fufvTrange)))}.reduce((a,b) => (a._1+b._1,a._2+b._2))
      (ux, kq._2 - sf + fu,kq._1 - fusfT + fufuT)//(u,grad,llh)
    }.persist()
    // BACKTRACKING LINE SEARCH

    var kq1 = result1.cartesian(liststepSizeRDD).map{ case (x,stepx) =>
      var fuold = Flookup(Fbc,x._1)
      var newfu = step(fuold,stepx,x._2);
      val fuT = BDM.create(K.value,1,newfu.data);
      val sfT = BDM.create[Double](K.value, 1, sumF.data) - BDM.create(K.value,1,fuold.toArray) + fuT;
      var result = Neightborbc.value(x._1).map{m =>
        var xc = newfu*BDM.create(K.value,1,Flookup(Fbc,m).data)
        math.log(1- math.min(math.max(math.exp(-xc.data(0)),MIN_P_),MAX_P_))+ xc.data(0)
      }.reduce(_+_) -(newfu*sfT).data(0)+(newfu*fuT).data(0)
      (x._1,stepx,result >= (x._3 + (alpha*stepx*x._2 * BDM.create(K.value,1,x._2.data)).data.reduce(_+_)))
    }.filter(_._3).map(x => (x._1,x._2)).groupByKey().map(x => (x._1, x._2.max)).join(result1.keyBy(_._1)).map{case(u,(stepx,(ux, grad, llh))) =>
      var fu = Vectors.dense(step(Flookup(Fbc,u),stepx,grad).data).toSparse;
      (u,new BSV[Double](fu.indices,fu.values,K.value))}.persist()
    //UPDATE F and SUMF
    var Sx: Array[Long] = Array()
    if(kq1.count() != 0)
      {
        Sx = kq1.map(_._1).collect
        F = F.filter(x => !Sx.contains(x._1)).union(kq1.map(x=>(x._1,x._2))).cache
        var changeFu =kq1.map(x=>(FlookupSparseVector(Fbc,x._1),x._2)).reduce((a,b) => (a._1+ b._1,a._2+b._2))
        sumF = sumF - (changeFu._1 - changeFu._2).toDenseVector.toDenseMatrix
      }
    var fupdatebc = sc.broadcast(kq1.map(x=>(x._1,x._2)).collectAsMap())
    //compute loglikelihood
    val LLH = collectNeighbor.map{case(x,y)=>
      var fu : BDM[Double] = null
      if(Sx.contains(x))
        {
          fu = fupdatebc.value(x).toDenseVector.toDenseMatrix
        }else
        {
          fu =Flookup(Fbc,x)
        }
      val fusfT = (fu * BDM.create(K.value, 1, sumF.data)).data(0)
      val fufuT = (fu * BDM.create(K.value, 1, fu.data)).data(0)
      y.map {m =>
        var fv :BDM[Double] = null;
        if(Sx.contains(m))
        {
          fv = fupdatebc.value(m).toDenseVector.toDenseMatrix
        }else
        {
          fv =Flookup(Fbc,m)
        }
        var fvT = BDM.create(K.value, 1,fv.data);
        var fufvT = (fu * fvT).data(0);
        (math.log(1 - math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_))+ fufvT)}.reduce(_+_) - fusfT + fufuT
    }.reduce(_+_)
    //fupdatebc.destroy()
    //Fbc.destroy()
    return LLH
  }

  def MBSGD():Unit={
    var LLHold = 0.0
    println("LLH: " + LLHold)
    var size = G.vertices.count()
    breakable {
      var i =0
      var uset = G.vertices.map(_._1).collect.toList
      while (true) {
        var newLLH = backtrackingLineSearchs(uset)
        i+= uset.size
        println(" Inter: "+ i + " LLH: " + newLLH )
        if (math.abs(1 - newLLH/ LLHold) < 0.0001) {break}
        LLHold = newLLH

      }
    }
  }

initNeighborComF
MBSGD()

