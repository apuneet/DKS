%%%%%%%%%%%%%%%%%%%%%% DEPENDENCIES %%%%%%%%%%%%%%%%%%%%%%%%
You need to have Java 6/7 installed
It works only on linux
It uses Hadoop 1.0.0 and Giraph binaries are already available in folder graph-search/MyPregel/build/
%%%%%%%%%%%%%%%%%%%%%%% BASIC Set Up %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
You need to set-up two folders for DKS to work

1. LOD_HOME on Local computer / edge computer which will be used for issueing the command to run

2. LOD_HOME on the HDFS (Hadooop Distributed File System). These two are different file-systems.

3. In both file systems (Local file system, and on HDFS) perform all the above steps. As we run any step, its a good idea to keep the two file systems in sync. When running any Hadoop /Giraph job, it may create extra folders such as _SUCCESS or _logs. Its a good idea to keep removing all such folder as we proceed to next steps.


4. create a folder for your data-package, e.g., sec-rdfabout in the folder LOD_HOME.
   create folder LOD_HOME/<dp-name>/data
   Copy your knowledge graph file(s) in this folder. 
   this data should be in triple format with extension *.nt
   There should be nothing else in this folder.

5. create folder LOD_HOME/<dp-name>/vertex_mapping
   create folder LOD_HOME/<dp-name>/logs
   create folder LOD_HOME/<dp-name>/GiraphInput
   create folder LOD_HOME/<dp-name>/Queries
   create folder LOD_HOME/<dp-name>/Scoring
   create folder LOD_HOME/<dp-name>/cleanData
   create folder LOD_HOME/<dp-name>/index

6. All the steps below take long time to run, depending on the size the data and the hadoop cluster.

7. 

%%%%%%%%%%%%%%%%%%%%% Environment Variables required  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

 

#Here B will indicate compile and build .jar file every time., R will indicate only run donot compile.
DEPLOY_MODE=B 

#LOD Home in the local machine.
LOD_HOME_LOC=/data/LOD_HOME/

#LOD Home in the HDFS, Check this path from HDFS browse interface.
LOD_HOME_SVR=/user/pa113939/LOD_ALL/

#Folder where you checked out this code.
Workspace_Home=/home/puneet/WinD/Workspaces/MyNewResearch/graph-search

#This 1 should be used if Hadoop is being used in Pseudo distributed mode or in distributed mode, otherwise use 0
isHadoopServer=1

%%%%%%%%%%%%%%%% Pre-Processing Steps %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

1. Generate vertex IDs. For this run the script ManipulateRDF/scr/generateRDFSub-2-VID-Mapping.sh
   Go to folder, ManipulateRDF/scr/ then run ./generateRDFSub-2-VID-Mapping.sh
   this will create some files in the folder LOD_HOME/<dp-name>/vertex_mapping
   content inside these files should look like following:


2. Go to folder, ManipulateRDF/scr/ and run the script ./CalculateScore.sh <data-package-name> <BLS>
   This program does many things, but only one of the output of this job is used by subsequent steps
   Which is 10th map-reduce job, in 3rd step. For details see the shell script of next step.
   
3. Sort the data for lucene index creation
   Go to folder, ManipulateRDF/scr/ and run the script ./AllSubsTogether.sh <data-package-name>
   this will create some files in the folder LOD_HOME/<dp-name>/cleanData

4. As suggested above, always keep local file system and HDFS in sync
   next, rename all the files part-xx of the folder LOD_HOME/<dp-name>/cleanData to have extension (suffix) .nt

5. Go to folder RDFSearch/scr
   run the script ./index.sh <data-package-name>
   this will create the lucene index in the folder - LOD_HOME/<dp-name>/index

6. Now create the edge list in following format. In this file, the first term and second term, separated by space are the vertex-IDs of various entities. The third term indicates the direction of the edge, and the fourth term separated by tab is the edge weight.

===============================
0.10083 1.6744 BKD	1.0
0.101 0.13123 BKD	2.0
0.10137 13.6756 BKD	1.0
0.10199 11.3395 BKD	1.0
===============================
1.6744 0.10083 FWD	1.0
0.13123 0.101 FWD	2.0
13.6756 0.10137 FWD	1.0
11.3395 0.10199 FWD	1.0
===============================

Its very important to upload these files in the HDFS, (again the best approach is the keep the two file systems in sync)

The above files should be kept in following folder LOD_HOME/<dp-name>/GiraphInput
%%%%%%%%%%%%%%%%%%%%%%%% RUN DKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

First time run with DEPLOY_MODE=B, and after that always run in DEPLOY_MODE=R

Go to folder MyPregel/scr/
run the scipt Pregel_DKS.sh, first run it without arguments, it will show help text.

The 100 queries used in the paper have been saved in following sell scripts

1) bluk-bnb		- 	F-bluk-bnb-100-1.sh
2) sec-rdfabout		-	F-sec-100-1.sh
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
