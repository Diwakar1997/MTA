
#echo "Total Arguments:" $#
#echo "All Arguments values:" $@
 
### Command arguments can be accessed as
 
#echo "First->"  $1
#echo "Second->" $2

var crdFilePath = $2
var topoFilePath = $3

var outputFilePath = $2
var outputFilePath1 = $3

if [ $1 == "Distance" ]
then
   time spark-submit --jars /home/ppr.gp1/Masking/netcdfAll-5.2.0.jar --class DriverPackage.Demo --master yarn --deploy-mode cluster --num-executors 17  --driver-memory 250g --executor-memory 30g --executor-cores 20 /home/ppr.gp1/Integrated/test_2.11-0.1.jar "INPUT_DATA/CRD100 ///bigdata/bigdata/home/ppr.gp1/INPUT_DATA/TOPOLOGY/4W5O_Aq3.prm.top a 1,2 $outputFilePath" $2 $1
elif [$1 =="AverageStructure"]
then
   time spark-submit --jars /home/ppr.gp1/Masking/netcdfAll-5.2.0.jar --class DriverPackage.Demo --master yarn --deploy-mode cluster --num-executors 17  --driver-memory 250g --executor-memory 30g --executor-cores 20 /home/ppr.gp1/Integrated/test_2.11-0.1.jar "INPUT_DATA/CRD100 ///bigdata/bigdata/home/ppr.gp1/INPUT_DATA/TOPOLOGY/4W5O_Aq3.prm.top $outputFilePath $outputFilePath1" $2 $1
elif [$1 =="GeneratePDB"]
then
   time spark-submit --jars /home/ppr.gp1/Masking/netcdfAll-5.2.0.jar --class DriverPackage.Demo --master yarn --deploy-mode cluster --num-executors 17  --driver-memory 250g --executor-memory 30g --executor-cores 20 /home/ppr.gp1/Integrated/test_2.11-0.1.jar "INPUT_DATA/CRD100 ///bigdata/bigdata/home/ppr.gp1/INPUT_DATA/TOPOLOGY/4W5O_Aq3.prm.top output/integrated_pdb output/integrated_pdbtime" $2 $1
elif [ $1 =="Dihedral" ]
then
  time spark-submit --jars /home/ppr.gp1/Masking/netcdfAll-5.2.0.jar --class DriverPackage.Demo --master yarn --deploy-mode cluster --num-executors 17  --driver-memory 250g --executor-memory 30g --executor-cores 20 /home/ppr.gp1/Integrated/test_2.11-0.1.jar "INPUT_DATA/CRD100/ ///bigdata/bigdata/home/ppr.gp1/INPUT_DATA/TOPOLOGY/4W5O_Aq3.prm.top $outputFilePath" $2 $1
else
   time spark-submit --jars /home/ppr.gp1/Masking/netcdfAll-5.2.0.jar --class DriverPackage.Demo --master yarn --deploy-mode cluster --num-executors 17  --driver-memory 250g --executor-memory 30g --executor-cores 20 /home/ppr.gp1/Integrated/test_2.11-0.1.jar "INPUT_DATA/CRD100/ ///bigdata/bigdata/home/ppr.gp1/INPUT_DATA/TOPOLOGY/4W5O_Aq3.prm.top $outputFilePath m 0,1,2" $2 $1

fi