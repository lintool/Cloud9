package edu.umd.cloud9.collection.trec;

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

public class TrecDocnoMapping implements DocnoMapping {

	private static final Logger sLogger = Logger.getLogger(TrecDocnoMapping.class);

	private String[] mDocids;

	public int getDocno(String docid) {
		return Arrays.binarySearch(mDocids, docid);
	}

	public String getDocid(int docno) {
		return mDocids[docno];
	}

	public void loadMapping(Path p, FileSystem fs) throws IOException {
		mDocids = TrecDocnoMapping.readDocnoData(p, fs);
	}

	static public void writeDocnoData(String input, String output) throws IOException {
		sLogger.info("Writing docno data to " + output);
		FSLineReader reader = new FSLineReader(input);
		List<String> list = new ArrayList<String>();

		sLogger.info("Reading " + input);
		int cnt = 0;
		Text line = new Text();
		while (reader.readLine(line) > 0) {
			String[] arr = line.toString().split("\\t");
			list.add(arr[0]);
			cnt++;
			if (cnt % 100000 == 0) {
				sLogger.info(cnt + " docs");
			}
		}
		reader.close();
		sLogger.info(cnt + " docs total. Done!");

		cnt = 0;
		sLogger.info("Writing " + output);
		FSDataOutputStream out = FileSystem.get(new Configuration()).create(new Path(output), true);
		out.writeInt(list.size());
		for (int i = 0; i < list.size(); i++) {
			out.writeUTF(list.get(i));
			cnt++;
			if (cnt % 100000 == 0) {
				sLogger.info(cnt + " docs");
			}
		}
		out.close();
		sLogger.info(cnt + " docs total. Done!");
	}

	static public String[] readDocnoData(Path p, FileSystem fs) throws IOException {
		FSDataInputStream in = fs.open(p);
		
		// docnos start at one, so we need an array that's one larger than
		// number of docs
		int sz = in.readInt() + 1;
		String[] arr = new String[sz];
		
		for (int i = 1; i < sz; i++) {
			arr[i] = in.readUTF();
		}
		in.close();

		return arr;
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("usage: (list|getDocno|getDocid) [mapping-file] [docid/docno]");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		System.out.println("loading mapping file " + args[1]);
		TrecDocnoMapping mapping = new TrecDocnoMapping();
		mapping.loadMapping(new Path(args[1]), fs);

		if (args[0].equals("list")) {
			for (int i = 1; i < mapping.mDocids.length; i++) {
				System.out.println(i + "\t" + mapping.mDocids[i]);
			}
		} else if (args[0].equals("getDocno")) {
			System.out.println("looking up docno for \"" + args[2] + "\"");
			int idx = mapping.getDocno(args[2]);
			if (idx > 0) {
				System.out.println(mapping.getDocno(args[2]));
			} else {
				System.err.print("Invalid docid!");
			}
		} else if (args[0].equals("getDocid")) {
			try {
				System.out.println("looking up docid for " + args[2]);
				System.out.println(mapping.getDocid(Integer.parseInt(args[2])));
			} catch (Exception e) {
				System.err.print("Invalid docno!");
			}
		} else {
			System.out.println("Invalid command!");
			System.out.println("usage: (list|getDocno|getDocid) [mapping-file] [docid/docno]");
		}
	}

}
