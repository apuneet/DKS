package com.res.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class HDFSFileManagement implements FileManagement {
	FileSystem fs = null;

	public static void main(String[] args) throws IOException {

		HDFSFileManagement fileMgr = new HDFSFileManagement(
				"/home/puneet/WinD/sw/hadoop-1.0.0/");
		// fileMgr.move(
		// "/user/pa113939/LOD_ALL/bluk-bnb/Queries/2-PearceDavidW-MyersNorman/temp/1-Residual/",
		// "/user/pa113939/LOD_ALL/bluk-bnb/Queries/2-PearceDavidW-MyersNorman/temp/1-MAP/",
		// "mapped", FileManagement.STARTS_WITH);
		fileMgr.copyToLocal(
				"/user/pa113939/LOD_ALL/sec-rdfabout/Queries/2-ralph-lexington/Answer/",
				"/data7/Puneet_A/LOD_HOME/sec-rdfabout/Queries/2-ralph-lexington/Answer/");
	}

	public HDFSFileManagement(String HADOOP_HOME) {
		Configuration conf = new Configuration();
		conf.addResource(HADOOP_HOME + "/conf/core-site.xml");
		conf.addResource(HADOOP_HOME + "/conf/hdfs-site.xml");
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void deleteFolder(String folderToDelete) {

		try {
			fs.delete(new Path(folderToDelete), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void move(String fromPath, String toPath, String pattern,
			int matchingType) {
		final String arPatt[] = new String[1];
		arPatt[0] = pattern;
		final int arMatchingType[] = new int[1];
		arMatchingType[0] = matchingType;

		Path sourcePath = new Path(fromPath);

		try {
			FileStatus[] arStatus = fs.listStatus(sourcePath, new PathFilter() {
				@Override
				public boolean accept(Path path) {
					if (arMatchingType[0] == STARTS_WITH
							&& path.getName().startsWith(arPatt[0])) {
						return true;
					}
					if (arMatchingType[0] == ENDS_WITH
							&& path.getName().endsWith(arPatt[0])) {
						return true;
					}
					if (arMatchingType[0] == CONTAINS
							&& path.getName().contains(arPatt[0])) {
						return true;
					}
					return false;
				}
			});
			System.out.println("To Move " + arStatus.length + " files.");
			fs.mkdirs(new Path(toPath));
			for (int i = 0; i < arStatus.length; i++) {
				FileStatus fStatus = arStatus[i];
				Path srcDFSPath = fStatus.getPath();
				System.out.println("Path Name=" + srcDFSPath.getName());
				Path toDFSPath = new Path(toPath + "/" + srcDFSPath.getName());
				boolean status = fs.rename(srcDFSPath, toDFSPath);
				System.out.println("Moved :  " + srcDFSPath + ", to: "
						+ toDFSPath + " Status=" + status);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void copyToLocal(String fromPathSVR, String toPathLOC) {
		try {
			FileStatus[] arStatus = fs.listStatus(new Path(fromPathSVR));
			if (arStatus == null || arStatus.length < 1) {
				return;
			}
			for (int i = 0; i < arStatus.length; i++) {
				FileStatus fStatus = arStatus[i];
				Path srcDFSPath = fStatus.getPath();
				fs.copyToLocalFile(srcDFSPath, new Path(toPathLOC));
			}

			System.out.println("Downloaded " + fromPathSVR
					+ ", to local filesystem - path: " + toPathLOC);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void copyToServer(String fromPathLOC, String toPathSVR) {
		try {
			fs.copyFromLocalFile(new Path(fromPathLOC), new Path(toPathSVR));
			System.out.println("Uploaded " + fromPathLOC + ", to HDFS - path: "
					+ toPathSVR);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
