#This script is used to gather all the triples of an rdf-resource together
#This script is used before creating inverted-index using Lucene.
#However the same program can be used through another script of the same task
if [ $# -lt 1 ] ; then
   echo "Error calling this script"
   echo "Usage $0 <DPName>"
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

outputPath=$DP_HOME_SVR/cleanData
dataInputPath=$DP_HOME_SVR/data
vertexMappingPath=$DP_HOME_SVR/vertex_mapping
inDegreePath=$DP_HOME_SVR/Scoring/BLS/10/
hadoop jar jars/ManipulateRDF.jar com.res.rdf.MainGatherTriplesWSameSubject $outputPath $dataInputPath $vertexMappingPath $inDegreePath
