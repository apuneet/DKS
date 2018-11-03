package my.res.search;

import java.io.File;

import my.res.err.MyException;

public class RDFSearchConfig {

	String dpName;
	String LOD_HOME;
	int resPerPage;
	String indexPath;

	public RDFSearchConfig(String dpName, String LOD_Home, int resPerPage)
			throws MyException {
		this.dpName = dpName;
		this.LOD_HOME = LOD_Home;
		this.resPerPage = resPerPage;
		indexPath = LOD_HOME + dpName + "/index";
		File f = new File(indexPath);
		if (!f.isDirectory() || f.isHidden() || !f.exists() || !f.canRead()) {
			throw new MyException("Pls recheck indexPath:" + indexPath);
		}
	}

	public String getIndexPath() {
		return indexPath;
	}

	public String getDpName() {
		return dpName;
	}

	public void setDpName(String dpName) {
		this.dpName = dpName;
	}

	public String getLOD_HOME() {
		return LOD_HOME;
	}

	public void setLOD_HOME(String lOD_HOME) {
		LOD_HOME = lOD_HOME;
	}

	public int getResPerPage() {
		return resPerPage;
	}

	public void setResPerPage(int resPerPage) {
		this.resPerPage = resPerPage;
	}

}
