
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Master {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MaskModules")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

//    val crdFile ="/home/ram/data_sets/MDS/INPUT_DATA/CRD/crd1-10.crd"
//    val topoFile = "/home/ram/data_sets/MDS/INPUT_DATA/Topology/topo.top"
    val crdFile ="/home/ram/data_sets/MDS/INPUT_DATA/CRD/crd1-10.crd"
    val topoFile = "/home/ram/data_sets/MDS/INPUT_DATA/Topology/topo.top"
    val pdbFile = "/home/ram/data_sets/MDS/INPUT_DATA/Topology/test5.pdb"
    val masking = new Mask(crdFile,topoFile)


    val dataBank = masking.getAtomDataFrame()
    dataBank.show()
    println("getAtomDataFrame count: "+dataBank.count())

    val df2 = masking.getMoleaculeDataFrame()
    println("getMoleaculeDataFrame count:- "+df2.count())
    df2.show()

    val df3 = masking.getMoleaculeIDDataSet()
    println("getMoleaculeIDDataSet count:- "+df3.count())
    df3.show()

    val df4 = masking.getProteinDataBank()
    println("Protein bank:"+df4.count())
    df4.show(175105)
    //val maskedDataFrame = masking.maskedByAtomMoleacule(proteinDataBank)
    //println("Count="+maskedDataFrame)
    //maskedDataFrame.show(500)

    println("End")
  }
}