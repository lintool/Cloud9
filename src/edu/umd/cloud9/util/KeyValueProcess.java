package edu.umd.cloud9.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public abstract class KeyValueProcess<K extends WritableComparable, V extends Writable> {
	private Map<String, Object> mHash = new HashMap<String, Object>();

	public KeyValueProcess() {
	}

	public abstract void process(K key, V value);

	public abstract void report();

	public void setProperty(String property, Object value) {
		mHash.put(property, value);
	}

	public Object getProperty(String property) {
		return mHash.get(property);
	}
}
