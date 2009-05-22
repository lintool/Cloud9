package edu.umd.cloud9.collection.clue;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.util.FSLineReader;
import edu.umd.cloud9.util.HMapKI;

public class ClueWarcDocnoMapping implements DocnoMapping {

	private static int[] sOffets = new int[13217];
	private static final HMapKI<String> sSubDirMapping = new HMapKI<String>();

	public int getDocno(String docid) {
		if (docid == null)
			return -1;

		String sec = docid.substring(10, 16);
		int secStart = sSubDirMapping.get(sec);

		int file = Integer.parseInt(docid.substring(17, 19));
		int cnt = Integer.parseInt(docid.substring(20, 25));

		int idx = secStart + file;
		int docno = sOffets[idx] + cnt;

		return docno;
	}

	public String getDocid(int docno) {
		throw new UnsupportedOperationException();
	}

	public void loadMapping(Path p, FileSystem fs) throws IOException {
		FSLineReader reader = new FSLineReader(p, fs);
		Text t = new Text();
		int cnt = 0;
		String prevSec = null;

		while (reader.readLine(t) > 0) {
			String[] arr = t.toString().split(",");

			if (prevSec == null || !arr[0].equals(prevSec)) {
				//System.out.println(cnt + " " + arr[0]);
				sSubDirMapping.put(arr[0], cnt);
			}

			sOffets[cnt] = Integer.parseInt(arr[3]);
			prevSec = arr[0];
			cnt++;
		}

		reader.close();
	}

}
