package edu.umd.cloud9.collection.clue;

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

public class ClueWarcForwardIndex implements DocumentForwardIndex<ClueWarcRecord> {

	private static final Logger sLogger = Logger.getLogger(ClueWarcForwardIndex.class);

	private Configuration mConf;
	private FileSystem mFS;

	private int[] mDocnos;
	private int[] mOffsets;
	private short[] mFileno;
	private String mCollectionPath;

	private ClueWarcDocnoMapping mDocnoMapping = new ClueWarcDocnoMapping();

	public ClueWarcForwardIndex() {
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

	public ClueWarcRecord getDocument(int docno) {
		long start = System.currentTimeMillis();
		int idx = Arrays.binarySearch(mDocnos, docno);

		if (idx < 0)
			idx = -idx - 2;

		DecimalFormat df = new DecimalFormat("00000");
		String file = mCollectionPath + "/part-" + df.format(mFileno[idx]);

		sLogger.info("fetching docno " + docno + ": seeking to " + mOffsets[idx] + " at " + file);

		try {

			SequenceFile.Reader reader = new SequenceFile.Reader(mFS, new Path(file), mConf);

			IntWritable key = new IntWritable();
			ClueWarcRecord value = new ClueWarcRecord();

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

	public ClueWarcRecord getDocument(String docid) {
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
		return "text/html";
	}

	public static void main(String[] args) throws Exception {
		ClueWarcForwardIndex f = new ClueWarcForwardIndex();

		f.loadIndex("/user/jimmylin/en.01.findex",
				"/umd/collections/ClueWeb09.repacked/docno-mapping.dat");

		ClueWarcRecord record;

		record = f.getDocument(1000);
		System.out.println(record.getDocid());

		record = f.getDocument(999964);
		System.out.println(record.getDocid());

		record = f.getDocument(1000000);
		System.out.println(record.getDocid());

		record = f.getDocument("clueweb09-en0000-00-00999");
		System.out.println(record.getDocid());

		record = f.getDocument("clueweb09-en0000-29-13277");
		System.out.println(record.getDocid());

		record = f.getDocument("clueweb09-en0000-29-13313");
		System.out.println(record.getDocid());
	}
}
