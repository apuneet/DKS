package com.iitd.gs.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MyDateUtils {
	public static void main(String[] args) throws ParseException {

		// Date d1 = getCurrentDateTime();
		String startDate = args[0], stopDate = args[1];
		// String startDate = "30-07-13::19:51:54::083";
		// String stopDate = "30-07-13::20:04:21::783";
		long miliSecDiff = dateDiff(startDate, stopDate);

		simplifyDiff(miliSecDiff);
	}

	public static Date getCurrentDateTime() {

		Calendar c1 = Calendar.getInstance();
		String d1 = c1.get(Calendar.DATE) + "-" + (c1.get(Calendar.MONTH) + 1)
				+ "-" + c1.get(Calendar.YEAR) + "::"
				+ c1.get(Calendar.HOUR_OF_DAY) + ":" + c1.get(Calendar.MINUTE)
				+ ":" + c1.get(Calendar.SECOND) + "::"
				+ c1.get(Calendar.MILLISECOND);
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yy::HH:mm:ss::S");
		Date dtDate = null;
		try {
			dtDate = sdf.parse(d1);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return dtDate;
	}

	public static long dateDiff(String startDate, String stopDate) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy::HH:mm:ss");
		Date dtStartDate = null;
		Date dtStopDate = null;
		try {
			dtStartDate = sdf.parse(startDate);
			dtStopDate = sdf.parse(stopDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		long diff = dtStopDate.getTime() - dtStartDate.getTime();
		return diff;
	}

	public static long dateDiff(Date startDate, Date stopDate) {
		if (startDate == null || stopDate == null) {
			return 0;
		}
		long diff = stopDate.getTime() - startDate.getTime();
		return diff;
	}

	public static String simplifyDiff(long milliSecDiff) {
		long secs = milliSecDiff / 1000;
		long min = secs / 60;
		long hrs = min / 60;
		long remMilli = milliSecDiff - secs * 1000;
		long remSec = secs - min * 60;
		long remMin = min - hrs * 60;
		String stDiff = hrs + ":" + remMin + ":" + remSec + "::" + remMilli;
		System.out.println(stDiff + "," + milliSecDiff);
		return stDiff;
	}

	public static void appendToFile(String fileName, String data) {
		try {
			FileWriter fstream = new FileWriter(fileName);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(data);
			out.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

}
