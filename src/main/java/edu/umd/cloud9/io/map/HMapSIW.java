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

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.util.map.HMapKI;
import edu.umd.cloud9.util.map.MapKI;

/**
 * Writable representing a map where keys are Strings and values are ints. This
 * class is specialized for String objects to avoid the overhead that comes with
 * wrapping Strings inside <code>Text</code> objects.
 * 
 * @author Jimmy Lin
 */
public class HMapSIW extends HMapKI<String> implements Writable {
	private static final long serialVersionUID = -9179978557431493856L;

	/**
	 * Creates a <code>HMapSIW</code> object.
	 */
	public HMapSIW() {
		super();
	}

	/**
	 * Deserializes the map.
	 *
	 * @param in source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		this.clear();

		int numEntries = in.readInt();
		if (numEntries == 0)
			return;

		for (int i = 0; i < numEntries; i++) {
			String k = in.readUTF();
			int v = in.readInt();
			put(k, v);
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

		// Then write out each key/value pair.
		for (MapKI.Entry<String> e : entrySet()) {
			out.writeUTF(e.getKey());
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
	 * Creates a <code>HMapSIW</code> object from a <code>DataInput</code>.
	 *
	 * @param in source for reading the serialized representation
	 * @return a newly-created <code>OHMapSIW</code> object
	 * @throws IOException
	 */
	public static HMapSIW create(DataInput in) throws IOException {
		HMapSIW m = new HMapSIW();
		m.readFields(in);

		return m;
	}

	/**
	 * Creates a <code>HMapSIW</code> object from a byte array.
	 *
	 * @param bytes source for reading the serialized representation
	 * @return a newly-created <code>OHMapSIW</code> object
	 * @throws IOException
	 */
	public static HMapSIW create(byte[] bytes) throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}
}
