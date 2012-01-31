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

package edu.umd.cloud9.webgraph.data;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.DocumentForwardIndex;
import edu.umd.cloud9.io.array.ArrayListWritable;

public class IndexableAnchorTextForwardIndex implements DocumentForwardIndex<IndexableAnchorText> {
	
	private static final IndexableAnchorText indexableAnchorText = new IndexableAnchorText();
	private static final DecimalFormat df = new DecimalFormat("00000");

	private Configuration conf;
	private FileSystem fs;

	private int[] docnos;
	private int[] offsets;
	private short[] filenos;
	private String collectionPath;

	private DocnoMapping docnoMapping;

	public IndexableAnchorTextForwardIndex(DocnoMapping docnoMapping) {
		this.docnoMapping = docnoMapping; 
	}

	@Override
	public void loadIndex(Path index, Path mapping, FileSystem fs) throws IOException {
		docnoMapping.loadMapping(mapping, fs);

		FSDataInputStream in = fs.open(index);

		// class name; throw away
		in.readUTF();
		collectionPath = in.readUTF();

		int blocks = in.readInt();

		docnos = new int[blocks];
		offsets = new int[blocks];
		filenos = new short[blocks];

		for (int i = 0; i < blocks; i++) {
			docnos[i] = in.readInt();
			offsets[i] = in.readInt();
			filenos[i] = in.readShort();
		}

		in.close();
	}

	public String getCollectionPath() {
		return collectionPath;
	}

	public IndexableAnchorText getDocument(int docno) {
		int idx = Arrays.binarySearch(docnos, docno);

		if (idx < 0)
			idx = -idx - 2;

		DecimalFormat df = new DecimalFormat("00000");
		String file = collectionPath + "/part-" + df.format(filenos[idx]);
		
		try {

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file), conf);

			IntWritable key = new IntWritable();
			ArrayListWritable<AnchorText> value = new ArrayListWritable<AnchorText>();

			reader.seek(offsets[idx]);

			while (reader.next(key)) {
				if (key.get() == docno)
					break;
			}

			reader.getCurrentValue(value);
			reader.close();
			
			indexableAnchorText.createHTML(value);
			return indexableAnchorText;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public IndexableAnchorText getDocument(String docid) {
		return getDocument(docnoMapping.getDocno(docid));
	}

	public int getDocno(String docid) {
		return docnoMapping.getDocno(docid);
	}

	public String getDocid(int docno) {
		return docnoMapping.getDocid(docno);
	}

	public int getFirstDocno() {
		return docnos[0];
	}

	private int mLastDocno = -1;

	public int getLastDocno() {
		if (mLastDocno != -1)
			return mLastDocno;

		// find the last entry, and then see all the way to the end of the
		// collection
		int idx = docnos.length - 1;

		String file = collectionPath + "/part-" + df.format(filenos[idx]);

		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file), conf);
			IntWritable key = new IntWritable();

			reader.seek(offsets[idx]);

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
