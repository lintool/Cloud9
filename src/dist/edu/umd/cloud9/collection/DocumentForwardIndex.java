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

	public int getDocno(String docid);

	public String getDocid(int docno);

	public T getDocument(int docno);
	
	public T getDocument(String docid);
	
	public int getFirstDocno();
	
	public int getLastDocno();
		
	public void loadIndex(String indexFile, String mappingDataFile)
			throws IOException;
	
	public String getCollectionPath();
}
