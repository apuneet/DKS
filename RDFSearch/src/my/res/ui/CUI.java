package my.res.ui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import my.res.Constants;
import my.res.err.MyException;
import my.res.search.RDFSearchConfig;
import my.res.search.RDFSearcher;

public class CUI {

	public static void main(String[] args) throws MyException {
		if (args == null || args.length < 1) {
			System.err
					.println("Usage: java my.res.search.RDFSearcher <dataPackage-name>");
			return;
		}
		String dpName = args[0];
		int resultsPerPage = Constants.resultsPerPage;
		String LOD_Home = System.getProperty("LOD_HOME");
		if (LOD_Home == null || LOD_Home.equals("") || !LOD_Home.endsWith("/")) {
			System.err
					.println("Set the Env Variable LOD_HOME ending with \"/\"");
			return;
		}
		String stResPerPage = System.getProperty("ResPerPage");
		if (stResPerPage != null && !stResPerPage.equals("")) {
			int a = Integer.parseInt(stResPerPage);
			resultsPerPage = a;
		}

		RDFSearchConfig searchConfig = new RDFSearchConfig(dpName, LOD_Home,
				resultsPerPage);
		CUI cui = new CUI();
		cui.CUIController(searchConfig);
	}

	public void CUIController(RDFSearchConfig searchConfig) {
		RDFSearcher mySearcher = new RDFSearcher(searchConfig);

		ResultsBean resultBean = null;
		while (true) {
			InputBean inputBean = getNextCommand();
			if (inputBean.getCommandType() == InputBean.Exit) {
				break;
			}
			if (!validateInput(inputBean, resultBean)) {
				continue;
			}
			resultBean = getNewResultsBean(inputBean, resultBean, searchConfig);
			if (inputBean.commandType == InputBean.NextPage
					|| inputBean.commandType == InputBean.PrevPage) {
				inputBean.setNextQuery(resultBean.getInputQuery());
			}
			resultBean = mySearcher.getResults(inputBean.getNextQuery(),
					resultBean);
			printPage(resultBean);
		}
	}

	private boolean validateInput(InputBean ib, ResultsBean oldResBean) {
		boolean goodInput = true;
		if (ib.getCommandType() == InputBean.NewQuery) {
			return goodInput;
		}
		if (ib.commandType == InputBean.NextPage
				&& (oldResBean == null || oldResBean.getInputQuery() == null)) {
			System.out.println("Please input the query text");
			goodInput = false;
		}
		if (oldResBean == null) {
			return goodInput;
		}
		int totalHits = oldResBean.getTotalHits();
		int lastResSize = oldResBean.getAlResultDocs().size();
		SearchResultDoc lastResDoc = oldResBean.getAlResultDocs().get(
				lastResSize - 1);

		int lastResultIndex = lastResDoc.getSeqNo();
		if (lastResultIndex == totalHits
				&& ib.getCommandType() == InputBean.NextPage) {
			System.out.println("Already on Last Page");
			goodInput = false;
		}
		SearchResultDoc firstResDoc = oldResBean.getAlResultDocs().get(0);
		int firstResultIndex = firstResDoc.getSeqNo();
		if (firstResultIndex == 1 && ib.getCommandType() == InputBean.PrevPage) {
			System.out.println("Already on First Page");
			goodInput = false;
		}

		return goodInput;
	}

	private ResultsBean getNewResultsBean(InputBean ib,
			ResultsBean oldResultBean, RDFSearchConfig searchConfig) {
		ResultsBean newResBean = new ResultsBean();
		newResBean.setResCount(searchConfig.getResPerPage());
		int startIndex = 0;
		if (ib.getCommandType() == InputBean.NewQuery) {
			newResBean.setStartIndex(1);
			newResBean.setCurrPageNo(1);
			newResBean.setInputQuery(ib.getNextQuery());
		} else if (ib.getCommandType() == InputBean.NextPage) {
			startIndex = oldResultBean.getCurrPageNo()
					* searchConfig.getResPerPage() + 1;
			newResBean.setStartIndex(startIndex);
			newResBean.setCurrPageNo(oldResultBean.getCurrPageNo() + 1);
			newResBean.setTotalHits(oldResultBean.getTotalHits());
			newResBean.setInputQuery(oldResultBean.getInputQuery());
		} else if (ib.getCommandType() == InputBean.PrevPage) {
			startIndex = (oldResultBean.getCurrPageNo() - 2)
					* searchConfig.getResPerPage() + 1;
			newResBean.setCurrPageNo(oldResultBean.getCurrPageNo() - 1);
			newResBean.setStartIndex(startIndex);
			newResBean.setTotalHits(oldResultBean.getTotalHits());
			newResBean.setInputQuery(oldResultBean.getInputQuery());
		}
		return newResBean;
	}

	private InputBean getNextCommand() {
		InputBean ib = new InputBean();
		BufferedReader in = null;
		String command = "Enter n - next, p - previous, e - exit or Next Query String";
		System.out.print(command + ":");
		try {
			in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
			String line = in.readLine();
			if (line != null && (line.equals("") || line.equals("n"))) {
				ib.setCommandType(InputBean.NextPage);
			} else if (line != null && line.equals("p")) {
				ib.setCommandType(InputBean.PrevPage);
			} else if (line == null || line.equals("e")) {
				ib.setCommandType(InputBean.Exit);
			} else {
				ib.setNextQuery(line);
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ib;
	}

	private void printPage(ResultsBean rBean) {

		if (rBean.getTotalHits() == 0) {
			System.out.println("============" + "No Results Found"
					+ "===========");
			return;
		}

		System.out
				.println("=====================================================================================");
		System.out.println("Start of Page # " + rBean.getCurrPageNo());
		for (SearchResultDoc sRes : rBean.getAlResultDocs()) {
			printAResult(sRes);
			System.out
					.println("----------------------------------------------------------------------------------------------");
		}
		System.out.print("End of Page # " + rBean.getCurrPageNo());
		System.out.print(", Total Hits:" + rBean.getTotalHits());
		System.out.println(", Query String:" + rBean.getInputQuery());
		System.out
				.println("=====================================================================================");
	}

	private void printAResult(SearchResultDoc sRes) {
		System.out.println(sRes.getSeqNo() + ". ================== doc="
				+ sRes.getDocID() + ", score=" + sRes.getSearchScore());
		System.out.println("DPName= ***********************************"
				+ sRes.getDpName() + "***********************************");
		System.out.println("FileName=" + sRes.getFileName());
		System.out.println("Subject=" + sRes.getSubject());
		System.out.println("InDegree=" + sRes.getInDegree());
		String[] objs = sRes.getObjects();
		String[] preds = sRes.getPredicates();
		System.out.println("Predicate                ----             Object");
		System.out.println("------------------------------------------------");
		for (int j = 0; j < objs.length; j++) {
			System.out.println(preds[j] + " ---- " + objs[j]);
		}
	}

}
