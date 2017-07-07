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
import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.broadcast.Broadcast

var G = GraphLoader.edgeListFile(sc, "file:///home/thangdn/Desktop/Email-Enron.txt")

val edgeList = G.ops.collectEdges(EdgeDirection.Out).map(x => x._2).flatMap(y =>y).collect().map(x=>Edge(x.dstId,x.srcId,x.attr)) ++ G.ops.collectEdges(EdgeDirection.In).map(x => x._2).flatMap(y =>y).collect()
    //move edgelist into RDD
val edgeListrdd = sc.parallelize(edgeList)
    // Convert G as an adjacency matrix
val A = new CoordinateMatrix(edgeListrdd.map(x => MatrixEntry(x.srcId,x.dstId,x.attr)))

var K = sc.broadcast(100)
var F:RDD[(Long,BDM[Double])] = _
var sumF:BDM[Double] = _

var sumF:BDM[Double] = null
val epsCommForce = 1e-6
val MIN_P_ = 0.0001
val MAX_P_ = 0.9999
val MIN_F_ = 0.0
val MAX_F_ = 1000.0

val collectNeighbor = G.ops.collectNeighborIds(EdgeDirection.Either).cache()
var Neightborbc=sc.broadcast(collectNeighbor.collectAsMap())


def getEgoGraphNodes(): RDD[(VertexId, Array[VertexId])]={
    return collectNeighbor.map(x =>(x._1,Array(x._1)++x._2))
  }
val Neightborhoodbc = sc.broadcast(getEgoGraphNodes().collectAsMap())
  /*best method for compute Conductance*/
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

var indx =collectNeighbor.map{case(x,y) => y.map(conducu => (conducu,ConductanceRDDbc.value(conducu))).min}.map{case(x,y)=>(y,x)}.sortByKey().map(_._2).collect.distinct
    sigmaDegres.destroy()
    return indx
  }

  def randomIndexedRow(index:Long,n : Int ):IndexedRow={
    IndexedRow(index.toLong,Vectors.dense(Array.fill(n)(Random.nextInt(2).toDouble)))
  }
  
  def initNeighborComF(): IndexedRowMatrix={
    // Get S set which is conductance of locally minimal
    var S = conductanceLocalMin()
    var rows = A.toIndexedRowMatrix.rows.filter{x=>
      S.contains(x.index)}.map{x =>
      val m =x.vector.toArray.updated(x.index.toInt,1.0);
      IndexedRow(x.index,Vectors.dense(m))}.zipWithIndex.map{case (x,y) =>
      IndexedRow(y,x.vector)
    }.map{case IndexedRow(x,y) => IndexedRow(x,y.toDense)}
    // If S set < K set then random K - S set
    var addRows:Array[IndexedRow] = null
    for (i <- (S.size) to (K.value-1)) {
      if (addRows == null) {
        addRows = Array(randomIndexedRow(i, A.numRows.toInt))
      }
      else {
        addRows ++= Array(randomIndexedRow(i, A.numRows.toInt))
      }
    }
    if (addRows != null){
      var addRowsRDD = sc.makeRDD(addRows)
      //addRowsRDD= addRowsRDD.map{case IndexedRow(x,y) => IndexedRow(x,y.toSparse)}
      rows = rows.union(addRowsRDD)
    }
    //Create matrix F
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)
    mat.toCoordinateMatrix.transpose.toIndexedRowMatrix
  }
  
var Ftemp = initNeighborComF
  var sumF = BDM.create(1,K.value,Ftemp.toRowMatrix().computeColumnSummaryStatistics().normL1.toArray)
  var F = Ftemp.rows.map(x => (x.index,BDM.create(1,K.value,x.vector.toArray)))


  def step(Fu:BDM[Double], stepSize:Double, direction:BDM[Double]):BDM[Double]= {
    BDM.create(1,K.value,(Fu + stepSize * direction).data.map{x =>
      math.min(math.max(x ,MIN_F_),MAX_F_)})
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
    var Fbc=sc.broadcast(F.collectAsMap())

    //PRE-BACKTRACKING LINE SEARCH
    var result1 = collectNeighbor.filter(x=> uset.contains(x._1)).map{case(ux,y)=>
      var fu = Fbc.value(ux);
      var fusfT = (fu*BDM.create(K.value,1,sumF.data)).data(0);
      var sf = sumF
      var fufuT = (fu*BDM.create(K.value,1,fu.data)).data(0);
      var kq =y.map {m =>
        var fv = Fbc.value(m);
        var fvT = BDM.create(K.value, 1, Fbc.value(m).data);
        var fufvT = (fu * fvT).data(0);
        var fufvTrange = math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_);
        (math.log(1- fufvTrange)+ fufvT,fv*(1/(1 - fufvTrange)))}.reduce((a,b) => (a._1+b._1,a._2+b._2))
      (ux,fu, kq._2 - sf + fu,kq._1 - fusfT + fufuT)//(u,fu,grad,llh)
    }.persist()
    // BACKTRACKING LINE SEARCH

    var kq1 = result1.cartesian(liststepSizeRDD).map{ case (x,stepx) =>
      var newfu = step(x._2,stepx,x._3);
      val fuT = BDM.create(K.value,1,newfu.data);
      val sfT = BDM.create[Double](K.value, 1, sumF.data) - BDM.create(K.value,1,x._2.data) + fuT;
      var result = Neightborbc.value(x._1).map{m =>
        var xc = newfu*BDM.create(K.value,1,Fbc.value(m.toInt).data)
        math.log(1- math.min(math.max(math.exp(-xc.data(0)),MIN_P_),MAX_P_))+ xc.data(0)
      }.reduce(_+_) -(newfu*sfT).data(0)+(newfu*fuT).data(0)
      (x._1,stepx,result >= (x._4 + (alpha*stepx*x._3 * BDM.create(K.value,1,x._3.data)).data.reduce(_+_)))
    }.filter(_._3).map(x => (x._1,x._2)).groupByKey().map(x => (x._1, x._2.max)).join(result1.keyBy(_._1)).map{case(u,(stepx,(ux, fu, grad, llh))) =>
      (u, fu,step(fu,stepx,grad))}.persist()
    //UPDATE F and SUMF
    var Sx: Array[Long] = Array()
    if(kq1.count() != 0)
      {
        Sx = kq1.map(_._1).collect
        F = F.filter(x => !Sx.contains(x._1)).union(kq1.map(x=> (x._1,x._3)))
        var changeFu =kq1.map(x=>(x._2,x._3)).reduce((a,b) => (a._1+ b._1,a._2+b._2))
        sumF = sumF - changeFu._1 + changeFu._2
      }
    var fupdatebc = sc.broadcast(kq1.map(x => (x._1,x._3)).collectAsMap())
    //compute loglikelihood
    val LLH = collectNeighbor.map{case(x,y)=>
      var fu : BDM[Double] = null
      if(Sx.contains(x))
        {
          fu = fupdatebc.value(x)
        }else
        {
          fu =Fbc.value(x)
        }
      val fusfT = (fu * BDM.create(K.value, 1, sumF.data)).data(0)
      val fufuT = (fu * BDM.create(K.value, 1, fu.data)).data(0)
      y.map {m =>
        var fv :BDM[Double] = null;
        if(Sx.contains(m))
        {
          fv = fupdatebc.value(m)
        }else
        {
          fv =Fbc.value(m)
        }
        var fvT = BDM.create(K.value, 1,fv.data);
        var fufvT = (fu * fvT).data(0);
        (math.log(1 - math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_))+ fufvT)}.reduce(_+_) - fusfT + fufuT
    }.reduce(_+_)

    Fbc.destroy()
    return LLH
  }

  def loglikelihood():Double ={
    var Fbc=sc.broadcast(F.collectAsMap())
    val result = collectNeighbor.map{case(x,y)=>
      val fu = Fbc.value(x)
      val fusfT = (fu * BDM.create(K.value, 1, sumF.data)).data(0)
      val fufuT = (fu * BDM.create(K.value, 1, fu.data)).data(0)
      y.map {m =>
        var fvT = BDM.create(K.value, 1, Fbc.value(m).data);
        var fufvT = (fu * fvT).data(0);
        (math.log(1 - math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_))+ fufvT)}.reduce(_+_) - fusfT + fufuT
    }.reduce(_+_)
    Fbc.destroy()
    return result
  }

  
  def MBSGD():Unit={
    var LLHold = loglikelihood()
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

MBSGD()

var e = 2.0*G.collectEdges(EdgeDirection.Either).count/(G.vertices.count*(G.vertices.count - 1))
    e = math.sqrt(-math.log(1-e))
    //(new IndexedRowMatrix(F.rows.map{x => IndexedRow(x.index,Vectors.dense(x.vector.toArray.map(value =>if (value>=e) 1.0 else 0.0)))})).toBlockMatrix.toLocalMatrix
    var Com = F.map{x => var Fmax = x._2.max; 
      (x._1, if(Fmax < e){
        Vectors.dense(x._2.map(value =>if (value == Fmax) 1.0 else 0.0).data).toSparse.indices}else{
        Vectors.dense(x._2.map(value =>if (value>=e) 1.0 else 0.0).data).toSparse.indices})}
    Com.flatMap{case(x,y) => y.map(c => (c,x))}.groupByKey().saveAsTextFile("file:///home/thangdn/Desktop/kq.txt")


