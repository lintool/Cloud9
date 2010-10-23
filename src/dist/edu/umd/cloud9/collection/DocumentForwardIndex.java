package edu.umd.cloud9.collection;

import java.io.IOException;

/**
 * Interface for a document forward index.
 * 
 * @author Jimmy Lin
 * 
 * @param <T>
 *            type of document
 */
public interface DocumentForwardIndex<T extends Indexable> {

	/** Returns the docno, given the docid */
	public int getDocno(String docid);

	/** Returns the docid, given the docno */
	public String getDocid(int docno);

	/** Fetches the document for a given docno. */
	public T getDocument(int docno);

	/** Fetches the document for a given docid. */
	public T getDocument(String docid);

	/** Returns the first docno in the collection. */
	public int getFirstDocno();
	
	/** Returns the last docno in the collection. */
	public int getLastDocno();

	/** Loads the index. */
	public void loadIndex(String indexFile, String mappingDataFile)
			throws IOException;
	
	/** Returns the base path of the collection. */
	public String getCollectionPath();
}
