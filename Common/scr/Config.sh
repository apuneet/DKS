# Assign B or T this variable. When its B, the system will build all jar files from source every-time it runs any command.
# When its T, it will use existing jar files.
_DEPLOY_MODE=B
_LOD_HOME_LOC=/data/Work-Homes/LOD_HOME/
_LOD_HOME_SVR=/user/puneet/LOD_ALL/
_Workspace_Home=/home/puneet/MyWorkspaces/DKS/graph-search


function setLibPaths()
{
  CLASSPATH=$CLASSPATH:$Workspace_Home/RDFSearch/lib/lucene-core-3.6.2.jar
  CLASSPATH=$CLASSPATH:$Workspace_Home/BiGKWS/lib/gson-2.2.2.jar
}

function setProjectPaths()
{
  if [ $DEPLOY_MODE == "B" ] ; then
     CLASSPATH=$CLASSPATH:$Workspace_Home/RDFSearch/bin
     CLASSPATH=$CLASSPATH:$Workspace_Home/ManipulateRDF/bin
     CLASSPATH=$CLASSPATH:$Workspace_Home/Common/bin
  else
     CLASSPATH=$CLASSPATH:$Workspace_Home/Common/jars/Common.jar
     CLASSPATH=$CLASSPATH:$Workspace_Home/RDFSearch/jars/RDFSearch.jar
     CLASSPATH=$CLASSPATH:$Workspace_Home/ManipulateRDF/jars/ManipulateRDF.jar
  fi
}

function setDKSProps()
{
   echo ".............Entering setBasicProps()"
   checkifServer
   if [ -z $DEPLOY_MODE ] ; then
     export DEPLOY_MODE=$_DEPLOY_MODE
   fi
   if [ -z $LOD_HOME_LOC ] ; then
     export LOD_HOME_LOC=$_LOD_HOME_LOC
   fi
   if [ -z $LOD_HOME_SVR ] ; then
     export LOD_HOME_SVR=$_LOD_HOME_SVR
   fi
   if [ -z $Workspace_Home ] ; then
     export Workspace_Home=$_Workspace_Home
   fi

   if [ $isServer -eq 0 ] ; then
     export LOD_HOME_SVR=$LOD_HOME_LOC
   fi
   echo "DEPLOY_MODE="$DEPLOY_MODE
   echo "LOD_HOME_LOC="$LOD_HOME_LOC
   echo "LOD_HOME_SVR="$LOD_HOME_SVR
   echo "Workspace_Home="$Workspace_Home

   echo ".............Exiting setBasicProps()"
}

function setDPHome()
{
   export DP_HOME_LOC=$LOD_HOME_LOC"$1""/"
   export DP_HOME_SVR=$LOD_HOME_SVR"$1""/"
   echo "DP_HOME_LOC="$DP_HOME_LOC
   echo "DP_HOME_SVR="$DP_HOME_SVR
}

function checkifServer()
{
   if [ $isHadoopServer -eq 0 ] ; then
     export isServer=0
   else
     export isServer=1
   fi
   echo "isServer="$isServer
}
#START

if [ $CONFIG_RUN -eq 1 ] ; then
   echo "CONFIG_RUN="$CONFIG_RUN
   echo "Not running Config.sh .........................................."
   return;
fi
echo "Running Config.sh............................................"

export CONFIG_RUN=1;

setDKSProps
setLibPaths
setProjectPaths
echo "==========================================================================="
echo "Config.sh:Press Enter to continue..."
#read
