package edu.umd.cloud9.collection.medline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.util.FSLineReader;

public class MedlineDocnoMapping implements DocnoMapping {
	private static final Logger sLogger = Logger.getLogger(MedlineDocnoMapping.class);

	private int[] mPmids;

	public int getDocno(String docid) {
		// docnos are numbered starting from one
		return Arrays.binarySearch(mPmids, Integer.parseInt(docid));
	}

	public String getDocid(int docno) {
		// docnos are numbered starting from one
		return new Integer(mPmids[docno]).toString();
	}

	public void loadMapping(Path p, FileSystem fs) throws IOException {
		mPmids = MedlineDocnoMapping.readDocidData(p, fs);
	}

	static public void writeDocidData(String input, String output) throws IOException {
		sLogger.info("Writing docids to " + output);
		FSLineReader reader = new FSLineReader(input);
		List<Integer> list = new ArrayList<Integer>();

		sLogger.info("Reading " + input);
		int cnt = 0;
		Text line = new Text();
		while (reader.readLine(line) > 0) {
			String[] arr = line.toString().split("\\t");
			list.add(Integer.parseInt(arr[0]));
			cnt++;
			if (cnt % 500000 == 0) {
				sLogger.info(cnt);
			}
		}
		reader.close();
		sLogger.info("Done!");

		cnt = 0;
		sLogger.info("Writing " + output);
		FSDataOutputStream out = FileSystem.get(new Configuration()).create(new Path(output), true);
		out.writeInt(list.size());
		for (int i = 0; i < list.size(); i++) {
			out.writeInt(list.get(i));
			cnt++;
			if (cnt % 500000 == 0) {
				sLogger.info(cnt);
			}
		}
		out.close();
		sLogger.info("Done!");
	}

	static public int[] readDocidData(Path file, FileSystem fs) throws IOException {
		sLogger.info("Reading docid mapping...");

		FSDataInputStream in = fs.open(file);
		// docnos start at one, so we need an array that's one larger than
		// number of docs
		int sz = in.readInt() + 1;
		int[] arr = new int[sz];
		int cnt = 0;
		for (int i = 1; i < sz; i++) {
			arr[i] = in.readInt();

			if (i % 500000 == 0) {
				sLogger.info(i);
			}
			cnt++;
		}
		in.close();

		sLogger.info(cnt + " docid mappings read");
		return arr;
	}
}
