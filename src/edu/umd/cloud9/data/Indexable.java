package edu.umd.cloud9.data;

import org.apache.hadoop.io.Writable;

public interface Indexable extends Writable {
	public int getDocno();

	public String getContent();
}
