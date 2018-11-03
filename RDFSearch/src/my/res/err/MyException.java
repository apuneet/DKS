package my.res.err;

public class MyException extends Exception {

	private static final long serialVersionUID = 1L;

	public MyException(String message) {
		super("MyException: " + message);
	}

}
