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

import edu.umd.cloud9.util.map.HMapIF;
import edu.umd.cloud9.util.map.MapIF;

/**
 * <p>
 * Writable representing a map where keys are ints and values are floats.
 * </p>
 *
 * <p>
 * One notable feature of this class is the ability to support <i>lazy decoding</i>,
 * controlled by the {@link #setLazyDecodeFlag(boolean)} method. In lazy
 * decoding mode, when an object of this type is deserialized, key-value pairs
 * are not inserted into the map, but rather held in arrays. The reduces memory
 * used in cases where random access to values is not required. In lazy decoding
 * mode, the raw keys and values may be fetched by the {@link #getKeys()} and
 * {@link #getValues()} methods, respectively. The map can be subsequently
 * populated with the {@link #decode()} method.
 * </p>
 *
 * @author Jimmy Lin
 */
public class HMapIFW extends HMapIF implements Writable {

	private static boolean LazyDecode = false;
	private static final long serialVersionUID = 4760032853L;

	private int numEntries = 0;
	private int[] keys = null;
	private float[] values = null;

	/**
	 * Creates a <code>HMapIFW</code> object.
	 */
	public HMapIFW() {
		super();
	}

	/**
	 * Deserializes the map.
	 *
	 * @param in source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		this.clear();

		numEntries = in.readInt();
		if (numEntries == 0)
			return;

		if (LazyDecode) {
			// Lazy initialization: read into arrays.
			keys = new int[numEntries];
			values = new float[numEntries];

			for (int i = 0; i < numEntries; i++) {
				keys[i] = in.readInt();
				values[i] = in.readFloat();
			}
		} else {
			// Normal initialization; populate the map.
			for (int i = 0; i < numEntries; i++) {
				put(in.readInt(), in.readFloat());
			}
		}
	}

	/**
	 * In lazy decoding mode, populates the map with deserialized data.
	 * Otherwise, does nothing.
	 *
	 * @throws IOException
	 */
	public void decode() {
		if (keys == null)
			return;

		for (int i = 0; i < keys.length; i++) {
			put(keys[i], values[i]);
		}

		keys = null;
		values = null;
	}

	/**
	 * Returns whether or not this map has been decoded. If not in lazy decoding
	 * mode, this method always return <i>true</i>.
	 */
	public boolean isDecoded() {
		if (getLazyDecodeFlag() == false)
			return true;

		return keys == null;
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

		for (MapIF.Entry e : entrySet()) {
			out.writeInt(e.getKey());
			out.writeFloat(e.getValue());
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
	 * Creates a <code>HMapIFW</code> object from a <code>DataInput</code>.
	 *
	 * @param in source for reading the serialized representation
	 * @return a newly-created <code>HMapIFW</code> object
	 * @throws IOException
	 */
	public static HMapIFW create(DataInput in) throws IOException {
		HMapIFW m = new HMapIFW();
		m.readFields(in);

		return m;
	}

	/**
	 * Creates a <code>HMapIFW</code> object from a byte array.
	 *
	 * @param bytes raw serialized representation
	 * @return a newly-created <code>HMapIFW</code> object
	 * @throws IOException
	 */
	public static HMapIFW create(byte[] bytes) throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	/**
	 * Sets the lazy decoding flag.
	 */
	public static void setLazyDecodeFlag(boolean b) {
		LazyDecode = b;
	}

	/**
	 * Returns the value of the lazy decoding flag
	 */
	public static boolean getLazyDecodeFlag() {
		return LazyDecode;
	}

	/**
	 * In lazy decoding mode, returns an array of all the keys if the map hasn't
	 * been decoded yet. Otherwise, returns null.
	 *
	 * @return an array of all the keys
	 */
	public int[] getKeys() {
		return keys;
	}

	/**
	 * In lazy decoding mode, returns an array of all the values if the map
	 * hasn't been decoded yet. Otherwise, returns null.
	 *
	 * @return an array of all the values
	 */
	public float[] getValues() {
		return values;
	}

	/**
	 * Adds values from keys of another map to this map. This map will be
	 * decoded if it hasn't already been decode. The other map need not be
	 * decoded.
	 *
	 * @param m the other map
	 */
	public void plus(HMapIFW m) {
		// This map must be decoded, so decode if it isn't already.
		if (!this.isDecoded())
			this.decode();

		if (!m.isDecoded()) {
			// If the other map hasn't been decoded, we can iterate through the arrays.
			int[] k = m.getKeys();
			float[] v = m.getValues();

			for (int i = 0; i < k.length; i++) {
				if (this.containsKey(k[i])) {
					this.put(k[i], this.get(k[i]) + v[i]);
				} else {
					this.put(k[i], v[i]);
				}
			}
		} else {
			// If the other map has already been decoded, the superclass plus
			// method can handle it.
			super.plus(m);
		}
	}

	@Override
	public int size() {
		if (!isDecoded()) {
			return keys.length;
		}

		return super.size();
	}
}
