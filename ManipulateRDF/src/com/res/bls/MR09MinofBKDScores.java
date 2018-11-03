package com.res.bls;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MR09MinofBKDScores {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			// <Subject> <Object> BKD Wt
			// <subject> <Object> FWD St
			String stInVal = inVal.toString();
			String stMapKey = "", stMapVal = "";

			StringTokenizer strTok = new StringTokenizer(stInVal);
			String stTerm1 = strTok.nextToken();
			String stTerm2 = strTok.nextToken();
			String stTerm3 = strTok.nextToken();
			String stTerm4 = strTok.nextToken();

			if (stTerm3.equals("FWD")) {
				stMapKey = stTerm2 + " " + stTerm1;
				stMapVal = stTerm3 + " :" + stTerm4;
				oc.collect(new Text(stMapKey), new Text(stMapVal));
				stMapKey = stTerm1 + " " + stTerm2;
				stMapVal = stTerm3 + " " + stTerm4;
			} else {
				stMapKey = stTerm1 + " " + stTerm2;
				stMapVal = stTerm3 + " " + stTerm4;
			}

			oc.collect(new Text(stMapKey), new Text(stMapVal));
		}
	}

	public static class ReduceResource extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text mappedKey, Iterator<Text> mappedItr,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			String stOrigKey = mappedKey.toString();
			String stValF1 = "", stValB = "", stValF2 = "";
			String nextVal = "", stVal = "";
			int count = 0;
			while (mappedItr.hasNext()) {
				count++;
				if (count > 2) {
					System.out.println("Fatal Error: Bug in BLS Logic");
					System.out
							.println("-------------------------------------------------------------------------");
					continue;
				}
				nextVal = mappedItr.next().toString();
				if (nextVal.startsWith("BKD")) {
					stValB = nextVal;
					continue;
				}
				if (stValF1.equals("")) {
					stValF1 = nextVal;
					continue;
				}
				stValF2 = nextVal;
			}

			if (!stValB.equals("") && !stValF1.equals("")) {
				String bkdWt = stValB.substring(3).trim();
				String fwdWt = stValF1.substring(5).trim();
				oc.collect(mappedKey, new Text("BKD " + bkdWt + ":" + fwdWt));
				StringTokenizer strTok = new StringTokenizer(stOrigKey);
				String vid1 = strTok.nextToken();
				String vid2 = strTok.nextToken();
				oc.collect(new Text(vid2 + " " + vid1), new Text("FWD " + fwdWt
						+ ":" + bkdWt));
			} else if (!stValF1.equals("") && !stValF2.equals("")) {
				String fwdWt1 = stValF1.substring(3).trim();
				String fwdWt2 = stValF2.substring(3).trim();
				if (fwdWt1.startsWith(":")) {
					stVal = "FWD " + fwdWt2 + "" + fwdWt1;
				} else {
					stVal = "FWD " + fwdWt1 + "" + fwdWt2;
				}
				oc.collect(mappedKey, new Text(stVal));
			} else if (!stValB.equals("") && stValF1.equals("")
					&& stValF2.equals("")) {
				System.out
						.println("===================================Only BKD Edge Present, no FWD Edge");
				System.out.println(mappedKey.toString() + " <==> " + stValB);
				System.out
						.println("=====================================================");
			} else if (stValB.equals("") && !stValF1.equals("")
					&& stValF2.equals("")) {
				// Such tuples are being ignored intentionally
			}

		}// end of reduce method
	} // end of reduce class
}
