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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * <p>
 * Interface that defines the callback associated with
 * {@link SequenceFileProcessor}. For each key-value pair, the
 * <code>SequenceFileProcessor</code> calls {@link #process}; this needs to
 * be instantiated by the user. After all the key-value pairs are processed,
 * <code>SequenceFileProcessor</code> calls {@link #report}; this also needs
 * to be instantiated by the user. Results of computations are retrieved using
 * {@link #getProperty(String)}.
 * </p>
 * 
 * @param <K>
 *            type of key
 * @param <V>
 *            type of value
 */
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
