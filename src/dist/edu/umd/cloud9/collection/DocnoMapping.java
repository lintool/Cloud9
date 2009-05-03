package edu.umd.cloud9.collection;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * <p>
 * Interface for an object that maps between <code>docid</code>s and
 * <code>docno</code>s. A <code>docid</code> is a globally-unique
 * identifier (String) for a document in the collection. For many types of
 * processing, documents in the collection must be sequentially numbered; thus,
 * each document in the collection must be assigned a unique integer identifier,
 * which is its <code>docno</code>.
 * </p>
 * 
 * <p>
 * Unless there are compelling reasons otherwise, it is preferable to start
 * numbering <code>docno</code>s from one instead of zero. This is because
 * zero cannot be represented in many common compression schemes that are used
 * in information retrieval (e.g., Golomb codes).
 * </p>
 */
public interface DocnoMapping {

	/**
	 * Returns the <code>docno<code> for a particular <code>docid</code>.
	 * @param docid the <code>docid</code>
	 * @return the <code>docno<code> for the <code>docid</code>
	 */
	public int getDocno(String docid);

	/**
	 * Returns the <code>docid<code> for a particular <code>docno</code>.
	 * @param docno the <code>docno</code>
	 * @return the <code>docid<code> for the <code>docno</code>
	 */
	public String getDocid(int docno);

	public void loadMapping(Path f, FileSystem fs) throws IOException;
}
