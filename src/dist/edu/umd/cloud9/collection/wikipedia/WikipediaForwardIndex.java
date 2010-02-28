package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocumentForwardIndex;

public class WikipediaForwardIndex implements DocumentForwardIndex<WikipediaPage> {

	private static final Logger sLogger = Logger.getLogger(WikipediaPage.class);

	private Configuration mConf;
	private FileSystem mFS;

	private int[] mDocnos;
	private int[] mOffsets;
	private short[] mFileno;
	private String mCollectionPath;

	private WikipediaDocnoMapping mDocnoMapping = new WikipediaDocnoMapping();

	public WikipediaForwardIndex() {
	}

	public void loadIndex(String indexFile, String mappingDataFile) throws IOException {
		sLogger.info("Loading forward index: " + indexFile);

		mConf = new Configuration();
		mFS = FileSystem.get(mConf);

		mDocnoMapping.loadMapping(new Path(mappingDataFile), mFS);

		FSDataInputStream in = mFS.open(new Path(indexFile));

		// class name; throw away
		in.readUTF();
		mCollectionPath = in.readUTF();

		int blocks = in.readInt();

		sLogger.info(blocks + " blocks expected");
		mDocnos = new int[blocks];
		mOffsets = new int[blocks];
		mFileno = new short[blocks];

		for (int i = 0; i < blocks; i++) {
			mDocnos[i] = in.readInt();
			mOffsets[i] = in.readInt();
			mFileno[i] = in.readShort();

			if (i > 0 && i % 100000 == 0)
				sLogger.info(i + " blocks read");
		}

		in.close();
	}

	public String getCollectionPath() {
		return mCollectionPath;
	}

	public WikipediaPage getDocument(int docno) {
		long start = System.currentTimeMillis();

		// trap invalid docnos
		if (docno < getFirstDocno() || docno > getLastDocno())
			return null;

		int idx = Arrays.binarySearch(mDocnos, docno);

		if (idx < 0)
			idx = -idx - 2;

		DecimalFormat df = new DecimalFormat("00000");
		String file = mCollectionPath + "/part-" + df.format(mFileno[idx]);

		sLogger.info("fetching docno " + docno + ": seeking to " + mOffsets[idx] + " at " + file);

		try {

			SequenceFile.Reader reader = new SequenceFile.Reader(mFS, new Path(file), mConf);

			IntWritable key = new IntWritable();
			WikipediaPage value = new WikipediaPage();

			reader.seek(mOffsets[idx]);

			while (reader.next(key)) {
				// sLogger.info("at " + key);
				if (key.get() == docno)
					break;
			}

			reader.getCurrentValue(value);
			reader.close();

			long duration = System.currentTimeMillis() - start;

			sLogger.info(" docno " + docno + " fetched in " + duration + "ms");
			return value;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public WikipediaPage getDocument(String docid) {
		return getDocument(mDocnoMapping.getDocno(docid));
	}

	public int getDocno(String docid) {
		return mDocnoMapping.getDocno(docid);
	}

	public String getDocid(int docno) {
		return mDocnoMapping.getDocid(docno);
	}

	public int getFirstDocno() {
		return mDocnos[0];
	}

	private int mLastDocno = -1;

	public int getLastDocno() {
		if (mLastDocno != -1)
			return mLastDocno;

		// find the last entry, and then see all the way to the end of the
		// collection
		int idx = mDocnos.length - 1;

		DecimalFormat df = new DecimalFormat("00000");
		String file = mCollectionPath + "/part-" + df.format(mFileno[idx]);

		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(mFS, new Path(file), mConf);
			IntWritable key = new IntWritable();

			reader.seek(mOffsets[idx]);

			while (reader.next(key))
				;
			mLastDocno = key.get();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return mLastDocno;
	}

	public String getContentType() {
		return "text/plain";
	}

	public static void main(String[] args) throws Exception {
		WikipediaForwardIndex f = new WikipediaForwardIndex();

		f.loadIndex("/shared/Wikipedia/compressed.block/findex-en-20091202.dat",
				"/shared/Wikipedia/docno-en-20091202.dat");

		WikipediaPage page;

		page = f.getDocument(1000);
		System.out.println(page.getDocid() + ": " + page.getTitle());

		page = f.getDocument(100000);
		System.out.println(page.getDocid() + ": " + page.getTitle());

		page = f.getDocument("1873");
		System.out.println(page.getDocid() + ": " + page.getTitle());

		page = f.getDocument("133876");
		System.out.println(page.getDocid() + ": " + page.getTitle());

	}
}
