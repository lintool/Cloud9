package edu.umd.cloud9.collection;

import org.apache.hadoop.io.Writable;

/**
 * Interface for a document that can be indexed.
 */
public interface Indexable extends Writable {

	/**
	 * Returns the globally-unique String identifier of the document within the
	 * collection.
	 */
	public String getDocid();

	/**
	 * Returns the content of the document.
	 */
	public String getContent();
}
