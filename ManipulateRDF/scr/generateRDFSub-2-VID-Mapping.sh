#This script is used for generating the RDF-Subject to Vertex-ID mapping file.
if [ $# -lt 1 ] ; then
   echo "Error calling this script"
   echo "Usage $0 <DPName>"
   echo "This program takes the .nt file of RDF as input"
   exit 1;
fi

export CONFIG_RUN=0
. ./../../Common/scr/Config.sh
setDPHome $1

if [ $DEPLOY_MODE == "B" ] ; then
  cd ../bin
  jar -cvf ../jars/ManipulateRDF.jar *
fi
cd ..

inputPath=$DP_HOME_SVR/data
outputPath=$DP_HOME_SVR/vertex_mapping

nohup hadoop jar jars/ManipulateRDF.jar com.res.vid.GenRDFS2VIDMap $inputPath $outputPath 2>&1 >>$DP_HOME_LOC/logs/generateRDFSub-2-VID-Mapping.log &
