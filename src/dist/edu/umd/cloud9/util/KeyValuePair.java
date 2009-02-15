/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class representing a key-value pair.
 * 
 * @param <K>
 *            type of the key
 * @param <V>
 *            type of the value
 */
public class KeyValuePair<K extends WritableComparable, V extends Writable> {

	private K mKey;
	private V mValue;

	/**
	 * Creates a new key-value pair.
	 * 
	 * @param key
	 *            the key
	 * @param value
	 *            the value
	 */
	public KeyValuePair(K key, V value) {
		mKey = key;
		mValue = value;
	}

	/**
	 * Returns the key.
	 * 
	 * @return the key
	 */
	public K getKey() {
		return mKey;
	}

	/**
	 * Returns the value.
	 * 
	 * @return the value
	 */
	public V getValue() {
		return mValue;
	}

}
