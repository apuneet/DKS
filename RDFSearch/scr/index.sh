if [ $# -lt 1 ] ; then
   echo "Error calling this script"
   echo "Usage $0 <Data-Package-Name>"
   exit 1;
fi 
export CONFIG_RUN=0
. ./../../Common/scr/Config.sh
setDPHome $1

if [ $DEPLOY_MODE == "B" ] ; then
  cd ../bin
  jar -cvf ../jars/RDFSearch.jar *
fi
cd ..

dpName=$1
cd $DP_HOME_LOC
if [ $? -eq 0 ] ; then
   pwd
   ls -tlra
   echo "Removing dir index, press enter to continue:"
   read
   rm -fr index
fi


CLASSPATH=$CLASSPATH:$Workspace_Home/RDFSearch/bin
CLASSPATH=$CLASSPATH:$Workspace_Home/RDFSearch/lib/lucene-core-3.6.2.jar
export CLASSPATH
echo "Started Generating Index............."
#nohup java -DLOD_HOME=$LOD_HOME_LOC my.res.search.RDFIndexer $dpName 2>&1 >$DP_HOME_LOC/logs/index.log &

echo nohup java -DLOD_HOME=$LOD_HOME_LOC my.res.search.RDFIndexer $dpName
java -DLOD_HOME=$LOD_HOME_LOC my.res.search.RDFIndexer $dpName
