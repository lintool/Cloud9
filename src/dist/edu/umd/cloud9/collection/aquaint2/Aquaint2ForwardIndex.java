package edu.umd.cloud9.collection.aquaint2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocumentForwardIndex;

/**
 * <p>
 * Object representing a document forward index for AQUAINT2 collections.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class Aquaint2ForwardIndex implements DocumentForwardIndex<Aquaint2Document> {

	private static final Logger sLogger = Logger.getLogger(Aquaint2ForwardIndex.class);
	{
		sLogger.setLevel (Level.WARN);
	}


	private long[] mOffsets;
	private int[] mLengths;
	private FSDataInputStream mCollectionStream;
	private Aquaint2DocnoMapping mDocnoMapping = new Aquaint2DocnoMapping();
	private String mCollectionPath;

	public int getDocno(String docid) {
		return mDocnoMapping.getDocno(docid);
	}

	public String getDocid(int docno) {
		return mDocnoMapping.getDocid(docno);
	}

	public int getLastDocno() {
		return mOffsets.length-1;
	}

	public int getFirstDocno() {
		return 1;
	}

	public String getCollectionPath() {
		return mCollectionPath;
	}

	public Aquaint2Document getDocument(String docid) {
		return getDocument(mDocnoMapping.getDocno(docid));
	}

	public Aquaint2Document getDocument(int docno) {
		Aquaint2Document doc = new Aquaint2Document();

		try {
			sLogger.debug("docno " + docno + ": byte offset " + mOffsets[docno] + ", length "
					+ mLengths[docno]);

			mCollectionStream.seek(mOffsets[docno]);

			byte[] arr = new byte[mLengths[docno]];

			mCollectionStream.read(arr);

			Aquaint2Document.readDocument(doc, new String(arr));
		} catch (IOException e) {
			e.printStackTrace();
		}

		return doc;
	}

	public void loadIndex(String indexFile, String mappingDataFile) throws IOException {
		Path p = new Path(indexFile);
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream in = fs.open(p);

		// read and throw away
		in.readUTF();
		mCollectionPath = in.readUTF();

		// docnos start at one, so we need an array that's one larger than
		// number of docs
		int sz = in.readInt() + 1;
		mOffsets = new long[sz];
		mLengths = new int[sz];

		for (int i = 1; i < sz; i++) {
			mOffsets[i] = in.readLong();
			mLengths[i] = in.readInt();
		}
		in.close();

		mCollectionStream = fs.open(new Path(mCollectionPath));
		mDocnoMapping.loadMapping(new Path(mappingDataFile), fs);
	}
}
