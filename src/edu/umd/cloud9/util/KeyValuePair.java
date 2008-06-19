package edu.umd.cloud9.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class KeyValuePair<K extends WritableComparable, V extends Writable> {

	private K mKey;
	private V mValue;
	
	public KeyValuePair(K key, V value) {
		mKey = key;
		mValue = value;
	}
	
	public K getKey() {
		return mKey;
	}
	
	public V getValue() {
		return mValue;
	}
	
	
}
