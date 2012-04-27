package edu.umd.cloud9.collection.aquaint2;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocumentForwardIndex;

/**
 * Object representing a document forward index for AQUAINT2 collections.
 *
 * @author Jimmy Lin
 */
public class Aquaint2ForwardIndex implements DocumentForwardIndex<Aquaint2Document> {
	private static final Logger LOG = Logger.getLogger(Aquaint2ForwardIndex.class);

	private long[] offsets;
	private int[] lengths;
	private FSDataInputStream input;
	private Aquaint2DocnoMapping docnoMapping = new Aquaint2DocnoMapping();
	private String collectionPath;

	@Override
	public int getDocno(String docid) {
		return docnoMapping.getDocno(docid);
	}

	@Override
	public String getDocid(int docno) {
		return docnoMapping.getDocid(docno);
	}

	@Override
	public int getLastDocno() {
		return offsets.length-1;
	}

	@Override
	public int getFirstDocno() {
		return 1;
	}

	@Override
	public String getCollectionPath() {
		return collectionPath;
	}

	@Override
	public Aquaint2Document getDocument(String docid) {
		return getDocument(docnoMapping.getDocno(docid));
	}

	@Override
	public Aquaint2Document getDocument(int docno) {
		Aquaint2Document doc = new Aquaint2Document();

		try {
			LOG.debug("docno " + docno + ": byte offset " + offsets[docno] + ", length "
					+ lengths[docno]);

			input.seek(offsets[docno]);

			byte[] arr = new byte[lengths[docno]];

			input.read(arr);

			Aquaint2Document.readDocument(doc, new String(arr));
		} catch (IOException e) {
			e.printStackTrace();
		}

		return doc;
	}

	@Override
	public void loadIndex(Path index, Path mapping, FileSystem fs) throws IOException {
		FSDataInputStream in = fs.open(index);

		// Read and throw away.
		in.readUTF();
		collectionPath = in.readUTF();

		// Docnos start at one, so we need an array that's one larger than number of docs.
		int sz = in.readInt() + 1;
		offsets = new long[sz];
		lengths = new int[sz];

		for (int i = 1; i < sz; i++) {
			offsets[i] = in.readLong();
			lengths[i] = in.readInt();
		}
		in.close();

		input = fs.open(new Path(collectionPath));
		docnoMapping.loadMapping(mapping, fs);
	}
}
