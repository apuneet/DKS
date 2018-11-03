if [ $# -lt 2 ] ; then
   echo "Error calling this script"
   echo "USAGE: $0 <Data-Package-Name> <BLS/PSO>"
   exit 1;
fi

export CONFIG_RUN=0
. ../../Common/scr/Config.sh
setDPHome $1

dpName=$1
ScoringScheme=$2

if [ $ScoringScheme != "PSO" -a $ScoringScheme != "BLS" -a $ScoringScheme != "REF" ] ; then
   echo "Error calling this script"
   echo "USAGE: $0 <Data-Package-Name> <BLS/PSO/REF>"
   echo "For Pregel Based DKS, REF should be used."
   exit 1;
fi

if [ $DEPLOY_MODE == "B" ] ; then
  echo "Building Jar File...."
  cd $Workspace_Home/ManipulateRDF/bin
  jar -cvf ../jars/ManipulateRDF.jar *
  cd -
  echo "Jar File is ready...."
fi

DP_HOME=$LOD_HOME_SVR"/"$dpName
inputPath=$DP_HOME/data
outputPath=$DP_HOME"/Scoring/"$ScoringScheme
vidMapping=$DP_HOME"/vertex_mapping"
blsoutput=$outputPath"/08"
echo "Press Enter to Continue........."
read


if [ $ScoringScheme == "PSO" ] ; then
  hadoop jar $Workspace_Home/ManipulateRDF/jars/ManipulateRDF.jar com.res.sco.MainScoring $outputPath $inputPath
fi
if [ $ScoringScheme == "BLS" ] ; then
  hadoop jar $Workspace_Home/ManipulateRDF/jars/ManipulateRDF.jar com.res.bls.MainScoring $outputPath $inputPath 2>&1 >$DP_HOME_LOC/logs/BLS.log
  hadoop jar $Workspace_Home/ManipulateRDF/jars/ManipulateRDF.jar com.res.bls.vid.EdgeList $vidMapping $blsoutput 2>&1 >>$DP_HOME_LOC/logs/BLS.log
  nohup hadoop jar $Workspace_Home/ManipulateRDF/jars/ManipulateRDF.jar com.res.bls.MainScoring2 $outputPath 2>&1 >>$DP_HOME_LOC/logs/BLS.log &
fi
if [ $ScoringScheme == "REF" ] ; then
  hadoop jar $Workspace_Home/ManipulateRDF/jars/ManipulateRDF.jar com.res.bls.MainScoring $outputPath $inputPath 2>&1 >$DP_HOME_LOC/logs/BLS.log
  hadoop jar $Workspace_Home/ManipulateRDF/jars/ManipulateRDF.jar com.res.bls.vid.EdgeList $vidMapping $blsoutput 2>&1 >>$DP_HOME_LOC/logs/BLS.log
  nohup hadoop jar $Workspace_Home/ManipulateRDF/jars/ManipulateRDF.jar com.res.bls.MainScoring2 $outputPath 2>&1 >>$DP_HOME_LOC/logs/BLS.log &
fi
