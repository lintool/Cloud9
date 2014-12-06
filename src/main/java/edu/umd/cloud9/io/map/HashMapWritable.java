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

package edu.umd.cloud9.io.map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * <p>
 * Writable extension of a Java HashMap. This generic class supports the use of
 * any type as either key or value. For a feature vector, {@link HMapKIW},
 * {@link HMapKFW}, and a family of related classes provides a more efficient
 * implementation.
 * </p>
 *
 * <p>
 * There are a number of key differences between this class and Hadoop's
 * {@link MapWritable}:
 * </p>
 *
 * <ul>
 *
 * <li><code>MapWritable</code> is more flexible in that it supports
 * heterogeneous elements. In this class, all keys must be of the same type and
 * all values must be of the same type. This assumption allows a simpler
 * serialization protocol and thus is more efficient. Run <code>main</code> in
 * this class for a simple efficiency test.</li>
 *
 * </ul>
 *
 * @param <K> type of the key
 * @param <V> type of the value
 *
 * @author Jimmy Lin
 * @author Tamer Elsayed
 */
public class HashMapWritable<K extends Writable, V extends Writable> extends HashMap<K, V> implements Writable {
	private static final long serialVersionUID = -7549423384046548469L;

	/**
	 * Creates a HashMapWritable object.
	 */
	public HashMapWritable() {
		super();
	}

	/**
	 * Creates a HashMapWritable object from a regular HashMap.
	 */
	public HashMapWritable(HashMap<K, V> map) {
		super(map);
	}

	/**
	 * Deserializes the array.
	 *
	 * @param in source for raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {

		this.clear();

		int numEntries = in.readInt();
		if (numEntries == 0)
			return;

		String keyClassName = in.readUTF();
		String valueClassName = in.readUTF();

		K objK;
		V objV;
		try {
			Class<K> keyClass = (Class<K>) Class.forName(keyClassName);
			Class<V> valueClass = (Class<V>) Class.forName(valueClassName);
			for (int i = 0; i < numEntries; i++) {
				objK = (K) keyClass.newInstance();
				objK.readFields(in);
				objV = (V) valueClass.newInstance();
				objV.readFields(in);
				put(objK, objV);
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Serializes this array.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		// Write out the number of entries in the map.
		out.writeInt(size());
		if (size() == 0)
			return;

		// Write out the class names for keys and values assuming that all entries have the same type.
		Set<Map.Entry<K, V>> entries = entrySet();
		Map.Entry<K, V> first = entries.iterator().next();
		K objK = first.getKey();
		V objV = first.getValue();
		out.writeUTF(objK.getClass().getCanonicalName());
		out.writeUTF(objV.getClass().getCanonicalName());

		// Then write out each key/value pair.
		for (Map.Entry<K, V> e : entrySet()) {
			e.getKey().write(out);
			e.getValue().write(out);
		}
	}
}
