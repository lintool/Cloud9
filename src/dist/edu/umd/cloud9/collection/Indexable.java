package edu.umd.cloud9.collection;

import org.apache.hadoop.io.Writable;

public interface Indexable extends Writable {
	public String getDocid();

	public String getContent();
}
