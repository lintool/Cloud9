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

import edu.umd.cloud9.util.map.HMapKI;
import edu.umd.cloud9.util.map.MapKI;

/**
 * Writable representing a map from keys of arbitrary WritableComparable to ints.
 *
 * @param <K> type of key
 *
 * @author Jimmy Lin
 */
public class HMapKIW<K extends WritableComparable<?>> extends HMapKI<K> implements Writable {
	private static final long serialVersionUID = -495714688553572924L;

	/**
	 * Creates a <code>HMapKIW</code> object.
	 */
	public HMapKIW() {
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

		String keyClassName = in.readUTF();

		K objK;

		try {
			Class<K> keyClass = (Class<K>) Class.forName(keyClassName);
			for (int i = 0; i < numEntries; i++) {
				objK = (K) keyClass.newInstance();
				objK.readFields(in);
				int s = in.readInt();
				put(objK, s);
			}
		} catch (Exception e) {
			throw new IOException("Unable to create HMapKIW!");
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

		// Write out the class names for keys and values assuming that all keys have the same type.
		Set<MapKI.Entry<K>> entries = entrySet();
		MapKI.Entry<K> first = entries.iterator().next();
		K objK = first.getKey();
		out.writeUTF(objK.getClass().getCanonicalName());

		// Then write out each key/value pair.
		for (MapKI.Entry<K> e : entrySet()) {
			e.getKey().write(out);
			out.writeInt(e.getValue());
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
	 * Creates a <code>HMapKIW</code> object from a <code>DataInput</code>.
	 *
	 * @param in source for reading the serialized representation
	 * @return a newly-created <code>HMapKIW</code> object
	 * @throws IOException
	 */
	public static <T extends WritableComparable<?>> HMapKIW<T> create(DataInput in) throws IOException {
		HMapKIW<T> m = new HMapKIW<T>();
		m.readFields(in);

		return m;
	}

	/**
	 * Creates a <code>HMapKIW</code> object from a byte array.
	 *
	 * @param bytes source for reading the serialized representation
	 * @return a newly-created <code>HMapKIW</code> object
	 * @throws IOException
	 */
	public static <T extends WritableComparable<?>> HMapKIW<T> create(byte[] bytes) throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}
}
