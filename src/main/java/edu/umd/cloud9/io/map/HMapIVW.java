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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.MapIV;

/**
 * Writable representing a map from ints to values of arbitrary WritableComparable.
 *
 * @param <V> type of value
 *
 * @author Jimmy Lin
 */
public class HMapIVW<V extends WritableComparable<?>> extends HMapIV<V> implements Writable {
	private static final long serialVersionUID = 2532109344100674110L;

	/**
	 * Creates a <code>HMapIVW</code> object.
	 */
	public HMapIVW() {
		super();
	}

	/**
	 * Deserializes the map.
	 *
	 * @param in source for raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		this.clear();

		int numEntries = in.readInt();
		if (numEntries == 0)
			return;

		String valueClassName = in.readUTF();

		V objV;

		try {
			Class<V> valueClass = (Class<V>) Class.forName(valueClassName);
			for (int i = 0; i < numEntries; i++) {
				int k = in.readInt();

				objV = (V) valueClass.newInstance();
				objV.readFields(in);

				put(k, objV);
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
	 * Serializes the map.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		// Write out the number of entries in the map.
		out.writeInt(size());
		if (size() == 0)
			return;

		// Write out the class names for keys and values, assuming that all entries have same types.
		Set<MapIV.Entry<V>> entries = entrySet();
		MapIV.Entry<V> first = entries.iterator().next();
		V objV = first.getValue();
		out.writeUTF(objV.getClass().getCanonicalName());

		// Then write out each key/value pair.
		for (MapIV.Entry<V> e : entrySet()) {
			out.writeInt(e.getKey());
			e.getValue().write(out);
		}
	}

	/**
	 * Returns the serialized representation of this object as a byte array.
	 *
	 * @return byte array representing the serialized representation of this object
	 * @throws IOException
	 */
	public byte[] serialize() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		write(dataOut);

		return bytesOut.toByteArray();
	}

	/**
	 * Creates a <code>HMapIVW</code> object from a <code>DataInput</code>.
	 *
	 * @param in source for reading the serialized representation
	 * @return a newly-created <code>HMapIVW</code> object
	 * @throws IOException
	 */
	public static <T extends WritableComparable<?>> HMapIVW<T> create(DataInput in) throws IOException {
		HMapIVW<T> m = new HMapIVW<T>();
		m.readFields(in);

		return m;
	}

	/**
	 * Creates a <code>HMapIVW</code> object from a byte array.
	 *
	 * @param bytes source for reading the serialized representation
	 * @return a newly-created <code>HMapIVW</code> object
	 * @throws IOException
	 */
	public static <T extends WritableComparable<?>> HMapIVW<T> create(byte[] bytes)	throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}
}
