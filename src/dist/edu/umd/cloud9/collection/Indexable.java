package edu.umd.cloud9.collection;

import org.apache.hadoop.io.Writable;

/**
 * Interface for a document that can be indexed.
 */
public interface Indexable extends Writable {
	public String getDocid();

	public String getContent();
}
