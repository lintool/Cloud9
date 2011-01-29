package edu.umd.cloud9.collection.clue;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.io.FSLineReader;
import edu.umd.cloud9.util.map.HMapKI;
import edu.umd.cloud9.util.map.MapKI;

/**
 * <p>
 * Object that maps between WARC-TREC-IDs (String identifiers) to docnos
 * (sequentially-numbered ints). This object provides mappings for the Clue Web
 * English collection; the docnos are numbered from part 1 all the way through
 * part 10.
 * </p>
 * <p>
 * Note that this class needs the data file <a href="docno.mapping"><code>docno.mapping</code></a>,
 * loaded via the {@link #loadMapping(Path, FileSystem)} method.
 * </p>
 * 
 * @author Jimmy Lin
 */
public class ClueWarcDocnoMapping implements DocnoMapping {

	private static final int[] sOffets = new int[13217];
	private static final HMapKI<String> sSubDirMapping = new HMapKI<String>();

	private static final NumberFormat sFormatW2 = new DecimalFormat("00");
	private static final NumberFormat sFormatW5 = new DecimalFormat("00000");

	/**
	 * Creates a <code>ClueWarcDocnoMapping</code> object
	 */
	public ClueWarcDocnoMapping() {
	}

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
		int i = 0;
		for (i = 0; i < sOffets.length; i++) {
			if (docno < sOffets[i])
				break;
		}
		i--;

		// System.out.println("offset = " + i + ", " + sOffets[i]);

		String docid = null;
		for (MapKI.Entry<String> e : sSubDirMapping.getEntriesSortedByValue()) {
			if (e.getValue() <= i) {
				// System.out.println(e.getKey() + "\t" + e.getValue());
				docid = "clueweb09-" + e.getKey() + "-" + sFormatW2.format(i - e.getValue()) + "-"
						+ sFormatW5.format(docno - sOffets[i]);
				break;
			}
		}

		return docid;
	}

	public void loadMapping(Path p, FileSystem fs) throws IOException {
		FSLineReader reader = new FSLineReader(p, fs);
		Text t = new Text();
		int cnt = 0;
		String prevSec = null;

		while (reader.readLine(t) > 0) {
			String[] arr = t.toString().split(",");

			if (prevSec == null || !arr[0].equals(prevSec)) {
				// System.out.println(cnt + " " + arr[0]);
				sSubDirMapping.put(arr[0], cnt);
			}

			sOffets[cnt] = Integer.parseInt(arr[3]);
			prevSec = arr[0];
			cnt++;
		}

		reader.close();
	}

}
