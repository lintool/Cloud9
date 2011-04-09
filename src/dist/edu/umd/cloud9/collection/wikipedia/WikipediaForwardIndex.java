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

/**
 * Forward index for Wikipedia collections.
 *
 * @author Jimmy Lin
 *
 */
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
		mConf = new Configuration();
	}

	public WikipediaForwardIndex(Configuration conf) {
		mConf = conf;
	}
	
	@Override
	public void loadIndex(String indexFile, String mappingDataFile) throws IOException {
		sLogger.info("Loading forward index: " + indexFile);

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

	@Override
	public String getCollectionPath() {
		return mCollectionPath;
	}

	@Override
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

//		sLogger.info("fetching docno " + docno + ": seeking to " + mOffsets[idx] + " at " + file);

		try {

			SequenceFile.Reader reader = new SequenceFile.Reader(mFS, new Path(file), mConf);

			IntWritable key = new IntWritable();
			WikipediaPage value = new WikipediaPage();

			reader.seek(mOffsets[idx]);

			while (reader.next(key)) {
//				sLogger.info("at " + key);
				if (key.get() == docno)
					break;
			}
//			sLogger.info("1 out of loop: "+key.get()+"\n"+docno);
			reader.getCurrentValue(value);
//			sLogger.info("2 out of loop: "+key.get()+"\n"+docno);
			reader.close();
//			sLogger.info("3 out of loop: "+key.get()+"\n"+docno);
			long duration = System.currentTimeMillis() - start;

//			sLogger.info(" docno " + docno + " fetched in " + duration + "ms");
			return value;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public WikipediaPage getDocument(String docid) {
		return getDocument(mDocnoMapping.getDocno(docid));
	}

	@Override
	public int getDocno(String docid) {
		return mDocnoMapping.getDocno(docid);
	}

	@Override
	public String getDocid(int docno) {
		return mDocnoMapping.getDocid(docno);
	}

	@Override
	public int getFirstDocno() {
		return mDocnos[0];
	}

	private int mLastDocno = -1;

	@Override
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
}
