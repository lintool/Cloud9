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

public class ClueWarcForwardIndex {

	private static final Logger sLogger = Logger.getLogger(ClueWarcForwardIndex.class);

	Configuration conf;
	FileSystem fs;

	private int[] mDocnos;
	private int[] mOffsets;
	private short[] mFileno;

	private String mCollectionPath;

	private ClueWarcDocnoMapping mDocnoMapping = new ClueWarcDocnoMapping();

	public ClueWarcForwardIndex(String indexFile, String collectionPath, String mappingDataFile)
			throws IOException {
		sLogger.info("Loading forward index: " + indexFile);

		mCollectionPath = collectionPath;

		conf = new Configuration();
		fs = FileSystem.get(conf);

		mDocnoMapping.loadMapping(new Path(mappingDataFile), fs);

		FSDataInputStream in = fs.open(new Path(indexFile));

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

	public ClueWarcRecord getDocno(int docno) {
		int idx = Arrays.binarySearch(mDocnos, docno);

		if (idx < 0)
			idx = -idx - 2;

		DecimalFormat df = new DecimalFormat("00000");
		String file = mCollectionPath + "/part-" + df.format(mFileno[idx]);

		sLogger.info("fetching " + docno + ": seeking to " + mOffsets[idx] + " at " + file);

		try {

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file), conf);

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

			return value;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public ClueWarcRecord getDocid(String docid) {
		return getDocno(mDocnoMapping.getDocno(docid));
	}

	public static void main(String[] args) throws Exception {
		ClueWarcForwardIndex f = new ClueWarcForwardIndex("/user/jimmylin/en.01.findex",
				"/umd/collections/ClueWeb09.repacked/en.01/",
				"/umd/collections/ClueWeb09.repacked/docno-mapping.dat");

		ClueWarcRecord record;

		record = f.getDocno(1000);
		System.out.println(record.getDocid());

		record = f.getDocno(999964);
		System.out.println(record.getDocid());

		record = f.getDocno(1000000);
		System.out.println(record.getDocid());
		
		record = f.getDocid("clueweb09-en0000-00-00999");
		System.out.println(record.getDocid());

		record = f.getDocid("clueweb09-en0000-29-13277");
		System.out.println(record.getDocid());

		record = f.getDocid("clueweb09-en0000-29-13313");
		System.out.println(record.getDocid());
	}
}
