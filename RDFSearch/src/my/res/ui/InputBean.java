package my.res.ui;

public class InputBean {
	public static final int NextPage = 1;
	public static final int PrevPage = 2;
	public static final int NewQuery = 3;
	public static final int Exit = 10;
	int commandType;
	String nextQuery;

	public int getCommandType() {
		return commandType;
	}

	public void setCommandType(int commandType) {
		this.commandType = commandType;
	}

	public String getNextQuery() {
		return nextQuery;
	}

	public void setNextQuery(String nextQuery) {
		this.commandType = NewQuery;
		this.nextQuery = nextQuery;
	}
}
