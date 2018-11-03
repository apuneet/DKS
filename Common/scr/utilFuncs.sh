_DEPLOY_MODE
_LOC_LOD_HOME
_SVR_LOD_HOME
_Workspace_Home

function checkifServer()
{
  echo "Entering checkifServer()"
  machineName=`hostname`
  echo "machineName="$machineName
  if [ $machineName == 'u-01HW344767' ] ; then
    isServer=0;
  else
    isServer=1;
  fi
  echo "isServer="$isServer
  echo "Exiting checkifServer()"
}
function setBasicProps()
{
   echo ".............Entering setBasicProps()"
   checkifServer
   if [ -z $BigSearch_Home ] ; then
     export BigSearch_Home=/home/puneet/WinC/BigKWS
   fi
   if [ -z $LOD_HOME ] ; then
#     export LOD_HOME=/01HW501421/WIP/LOD_Data/
     export LOD_HOME=/home/puneet/WinD/LOD_HOME/
   fi
   if [ $isServer -eq 1 ] ; then
     export LOD_HOME_SVR=/user/pa113939/LOD_ALL/
     export LOD_HOME_LOC=$LOD_HOME
   else
     export LOD_HOME_SVR=$LOD_HOME
     export LOD_HOME_LOC=$LOD_HOME
   fi

   export DP_HOME_LOC=$LOD_HOME_LOC"$1""/"
   export DP_HOME_SVR=$LOD_HOME_SVR"$1""/"
   echo "DP_HOME_LOC="$DP_HOME_LOC
   echo "DP_HOME_SVR="$DP_HOME_SVR
   echo ".............Exiting setBasicProps()"
}

