package com.res.util;

public interface FileManagement {
	public static final int STARTS_WITH = 0;
	public static final int CONTAINS = 1;
	public static final int ENDS_WITH = 2;

	void move(String fromPath, String toPath, String pattern, int matchingType);

	public void copyToLocal(String fromPathSVR, String toPathLOC);

	public void copyToServer(String fromPathLOC, String toPathSVR);

	public void deleteFolder(String folderToDelete);
}
