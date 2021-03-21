import java.io.{File, PrintWriter}

import scala.util.control.Breaks._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, floor, lit}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ucar.nc2._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

class Mask(crdFile:String,topoFile:String) {

  //Creating Spark Session.
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("MaskModules")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val topoDS = spark.read.textFile(topoFile)
  val zipdRDD = topoDS.rdd.zipWithIndex
//  val tempZip = zipdRDD
//  val zipdRDD1 = tempZip.map(x => (x._2,x._1))
  /**
   * Used to create DataFrame of coordinates
   * @param topoPath: Filepath of topology file.
   * @return DataFrame
   */
  def generateCRDDataSet(topoPath:String):DataFrame={
    val cordinate_read = NetcdfFile.open(topoPath).getVariables.get(2).read()
    val cordinates = ((0 to 300).map(x => cordinate_read.getFloat(x)))
    val cordinates_table = (0 to 300 by 3).map(x=> cordinates.slice(x, x+3)).toDS().select((0 until 3).map(i => col("value")(i)): _*)
    val cordinate_index = addColumnIndex(cordinates_table).toDF().withColumnRenamed("value[0]","X").withColumnRenamed("value[1]","Y").withColumnRenamed("value[2]","Z")
    cordinate_index
  }

  /**
   * Add index column with coordinates DataFrame
   * @param cordinates_table :DataFrame
   * @return DataFrame
   */
  def addColumnIndex(cordinates_table: DataFrame) = {
    spark.sqlContext.createDataFrame(cordinates_table.rdd.zipWithIndex.map {
      case (row, index) => Row.fromSeq(row.toSeq :+ index)},
      // Create schema for index column
      StructType(cordinates_table.schema.fields :+ StructField("index", LongType, false)))
  }

  /**
   * Generate Coordinate DataFrame with Frame number
   * @return Coordinate DataFrame with Frame number
   */
  def getCoordinateDataFrame(): DataFrame = {
    var noOfAtoms = 175105
    var noOfCrds = noOfAtoms * 3 * 10 // noOfAtoms * 3 * 10

    //Creating a coordinates dataset from the .crd file
    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    val filesList = getListOfFiles("/home/ram/data_sets/MDS/INPUT_DATA/CRD/")

    var noOfatoms = 175105
    var noOfframes = 1
    var crdArr = new ArrayBuffer[Float]()
    for (i <- 0 to noOfframes-1) {
      var filepath = filesList(i).toString()
      val cor = NetcdfFile.open(filepath).getVariables.get(2).read()
      var crdarrtmp = ((0 to 5253149).map(x => cor.getFloat(x))).toArray[Float]
      crdArr = crdArr ++ crdarrtmp
  }
    var totalCrd = 5253149 * noOfframes
    val crdRDD = (0 to totalCrd-1 by 3).map(x=> crdArr.slice(x, x+3)).toDS().select((0 until 3).map(i => col("value")(i)): _*)

    val crdDF =  spark.sqlContext.createDataFrame(
      crdRDD.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index+1)
      },
      StructType(crdRDD.schema.fields :+ StructField("crdIndex", LongType, false)))
      .withColumn("frame_no", lit(floor($"crdIndex"/noOfAtoms)))
      .withColumnRenamed("value[0]","x")
      .withColumnRenamed("value[1]","y")
      .withColumnRenamed("value[2]","z")
    crdDF
  }


  /**
   *
   * @return
   */
  def getAtomDataFrame():DataFrame ={
    val atomsLineDS = zipdRDD.filter(p => (p._2 >= 12 & p._2 <= 8767)).map(p => p._1).flatMap(p => p.split("(?<=\\G.{4})")).map(x=>x.trim())
      .zipWithIndex().map{ case (v, ind) => (v, ind + 1)}.toDS().cache()
    val atomDataSet = atomsLineDS.withColumnRenamed("_1","Atoms").withColumnRenamed("_2","index")
    atomDataSet
  }

  /**
   *
   * @return
   */
  def getMoleaculeDataFrame(): DataFrame ={

    val moleaculeDS = zipdRDD.filter(p => (p._2 >= 131397 & p._2 <= 134125)).map(p => p._1).flatMap(p => p.split("(?<=\\G.{4})")).map(x=>x.trim())
      .zipWithIndex().map { case (v, ind) => (v, ind + 1)}.toDS().cache()
    val moleaculeDataFrame = moleaculeDS.withColumnRenamed("_1","MoleaculeName").withColumnRenamed("_2","MoleaculeId")
    moleaculeDataFrame
  }

  /**
   *
   * @return
   */
  def getMoleaculeIDDataSet(): DataFrame ={
    var moleculeChainList = returnArray("%FLAG RESIDUE_POINTER", "%FLAG BOND_FORCE_CONSTANT",8)
    println("Array:"+moleculeChainList.size)
    var i = 0
    var moleaculeId = 1
    var list = List.empty[Int]

    while(i < moleculeChainList.size - 1) {
      var diff = moleculeChainList(i+1).toInt - moleculeChainList(i).toInt

      list = list.++(List.fill(diff)(moleaculeId))
      moleaculeId = moleaculeId + 1
      i = i + 1
    }
    println("List Count:"+list.size)
    val tempList = list.toList.toDS()
    val moleaculeIdDS = tempList.rdd.
      zipWithIndex().map{ case (v, ind) => (v, ind + 1)}.toDS().cache()
    val moleculeID = moleaculeIdDS.withColumnRenamed("_1","MoleaculeId").withColumnRenamed("_2","index")
   println("moleculeID":+moleculeID.count())
    moleculeID
  }

  /**
   *
   * @return
   */
  def getProteinDataBank(): DataFrame ={
    val atomMoleaculeIdDataFrame = getAtomDataFrame().join(getMoleaculeIDDataSet(), Seq("index"), "left")
println("atomMoleaculeIdDataFrame count:"+atomMoleaculeIdDataFrame.count())
    atomMoleaculeIdDataFrame.show()
    val atomMeleculeDataFrame =  atomMoleaculeIdDataFrame.join(getMoleaculeDataFrame(), Seq("MoleaculeId"), "left")
println("atomMeleculeDataFrame.count"+atomMeleculeDataFrame.count())
    atomMeleculeDataFrame.show()
    var crdDF = getCoordinateDataFrame().withColumn("index", lit(col("crdIndex") % 175105))
println("crdDF count:"+crdDF.count())
    crdDF.show()
    val proteinDataBank = atomMeleculeDataFrame.join(crdDF, Seq("index"), "left")
    proteinDataBank
//    proteinDataBank.orderBy("index")
  }


  def generatePDB(dataFrame: DataFrame,path:String): Unit ={
    var out = new PrintWriter(path) //lit(floor($"index"/noOfAtoms)
    val pdbSchema = dataFrame.withColumn("Atom",lit("ATOM")).withColumn("Col8",lit("1.00")).withColumn("col9",lit("0.00")).withColumn("col10",$"Atoms")
    pdbSchema.show()
    // pdbSchema.rdd.saveAsTextFile(path)
  }

  def maskedByAtomMoleacule(dataFrame:DataFrame): DataFrame ={
//    dataFrame.show()

    println("Enter Input:")
    var input = readLine()
    val moleaculeList = input.substring(0,input.indexOf('@')).split(" ").toList
    val atomList =  input.substring(input.indexOf('@')+1).split(" ").toList
    val maskedDataFrame = dataFrame.filter($"MoleaculeName".isin(moleaculeList:_*)).filter($"Atoms".isin(atomList:_*))

    maskedDataFrame
  }

  def maskedByAtom(dataFrame:DataFrame): DataFrame  ={
    println("Enter Input:")
    var input = readLine()
    val atomList  = input.split("\\s+").toList
    val maskedDataFrame = dataFrame.filter($"Atoms".isin(atomList:_*))
    maskedDataFrame
  }

  def maskedByMoleacule(dataFrame:DataFrame): DataFrame  ={
    println("Enter Input:")
    var input = readLine()
    val moleaculeList =  input.split("\\s+").toList
    val maskedDataFrame = dataFrame.filter($"MoleaculeName".isin(moleaculeList:_*))
    maskedDataFrame

  }
  /**
   *
   * @param startDelim
   * @param endDelim
   * @param s
   * @return
   */
  def returnArray(startDelim: String, endDelim: String,s: Int) : ListBuffer[String] = {
    val parmtop = Source.fromFile(topoFile)
    var itr = parmtop.getLines()
    var arr = new Array[String](5)
    var list = new ListBuffer[String]()
    var line = itr.next
    var x: Int = 1
    var count = 0
    while (itr.hasNext) {
      line = itr.next
      if (line.startsWith(startDelim)) {
        line = itr.next
        line = itr.next
        while (!line.startsWith(endDelim)) {
          try {
            count += 1
            arr = processLine(line, s)

            for (i <- 0 until arr.length) {
              var str =arr(i).trim()

              if(!str.equals("FLAG")) {
                //  println("found")
                list = list :+ str
              }
              x += 1
            }



          } catch {
            case x: Exception => println(x)
          }
          line = itr.next
        }

      }
    }

    list
  }

  def processLine(line: String, size: Int): Array[String] = {
    line.split("(?<=\\G.{" + size + "})")
  }


  def returnTermArray(startDelim: String, endDelim: String,s: Int) : ListBuffer[String] = {
    val parmtop = Source.fromFile(topoFile)
    var itr = parmtop.getLines()
    var arr = new Array[String](5)
    var list = new ListBuffer[String]()
    var line = itr.next
    var x: Int = 1
    var globalSum : Int = 0
    while (itr.hasNext) {
      line = itr.next
      if (line.startsWith(startDelim)) {
        line = itr.next
        line = itr.next
        while (!line.startsWith(endDelim)) {
          try {
            arr = processLine(line, s)
            for (i <- 0 until arr.length) {
              //println("Array" + " " + x + " " + arr(i) + " ")
              var temp = Integer.parseInt(arr(i).trim)
              globalSum += temp
              //println(globalSum)
              list = list :+ ""+globalSum
              x += 1
            }
          } catch {
            case x: Exception => println(x)
          }
          line = itr.next
        }
      }
    }
    list
  }


//  def demoFunc(): Unit = {
//    val startIndex = findIndex("FLAG ATOM_NAME")
//    val lastIndex = findIndex("FLAG CHARGE")
//
//    println(startIndex+" "+lastIndex)
//  }
//  def findIndex(del:String):Long={
//    var index:Long = 0
//    breakable {
//      for (i <- 0 until zipdRDD1.count().toInt) {
//        if (zipdRDD1.lookup(i).contains(del)) {
//          index = i
//          break
//        }
//      }
//    }
//    index
//
//  }
}
