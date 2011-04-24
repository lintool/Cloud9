/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.anchor.data;

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

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.DocumentForwardIndex;
import edu.umd.cloud9.io.array.ArrayListWritable;

public class IndexableAnchorTextForwardIndex implements DocumentForwardIndex<IndexableAnchorText> {

	private static final Logger sLogger = Logger.getLogger(IndexableAnchorTextForwardIndex.class);
	
	private static final IndexableAnchorText indexableAnchorText = new IndexableAnchorText();

	private Configuration mConf;
	private FileSystem mFS;

	private int[] mDocnos;
	private int[] mOffsets;
	private short[] mFileno;
	private String mCollectionPath;

	private DocnoMapping mDocnoMapping;

	public IndexableAnchorTextForwardIndex(DocnoMapping mDocnoMapping) {
		this.mDocnoMapping = mDocnoMapping; 
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

	public IndexableAnchorText getDocument(int docno) {
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
			ArrayListWritable<AnchorText> value = new ArrayListWritable<AnchorText>();

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
			
			indexableAnchorText.process(value);
			return indexableAnchorText;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public IndexableAnchorText getDocument(String docid) {
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
	
	public String getDisplayContentType() {
		return "text/html";
	}

}
