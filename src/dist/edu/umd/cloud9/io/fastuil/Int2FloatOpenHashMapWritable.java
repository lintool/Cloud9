package edu.umd.cloud9.io.fastuil;

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Int2FloatOpenHashMapWritable extends Int2FloatOpenHashMap implements Writable {

	private static final long serialVersionUID = 674980125439241L;

	/**
	 * Creates a <code>String2IntOpenHashMapWritable</code> object.
	 */
	public Int2FloatOpenHashMapWritable() {
		super();
	}

	/**
	 * Deserializes the map.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		this.clear();

		int numEntries = in.readInt();
		if (numEntries == 0)
			return;

		for (int i = 0; i < numEntries; i++) {
			int k = in.readInt();
			float v = in.readFloat();
			super.put(k, v);
		}
	}

	/**
	 * Serializes the map.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		// Write out the number of entries in the map
		out.writeInt(size());
		if (size() == 0)
			return;

		// Then write out each key/value pair
		for (Int2FloatMap.Entry e : int2FloatEntrySet()) {
			out.writeInt(e.getKey());
			out.writeFloat(e.getValue());
		}
	}

	/**
	 * Returns the serialized representation of this object as a byte array.
	 * 
	 * @return byte array representing the serialized representation of this
	 *         object
	 * @throws IOException
	 */
	public byte[] serialize() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		write(dataOut);

		return bytesOut.toByteArray();
	}

	/**
	 * Creates a <code>OHMapSIW</code> object from a <code>DataInput</code>.
	 * 
	 * @param in
	 *            <code>DataInput</code> for reading the serialized
	 *            representation
	 * @return a newly-created <code>OHMapSIW</code> object
	 * @throws IOException
	 */
	public static Int2FloatOpenHashMapWritable create(DataInput in) throws IOException {
		Int2FloatOpenHashMapWritable m = new Int2FloatOpenHashMapWritable();
		m.readFields(in);

		return m;
	}

	/**
	 * Returns the serialized representation of this object as a byte array.
	 * 
	 * @return byte array representing the serialized representation of this
	 *         object
	 * @throws IOException
	 */
	public static Int2FloatOpenHashMapWritable create(byte[] bytes) throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	/**
	 * Adds values of keys from another map to this map.
	 * 
	 * @param m
	 *            the other map
	 */
	public void plus(Int2FloatOpenHashMapWritable m) {
		for (Int2FloatMap.Entry e : m.int2FloatEntrySet()) {
			int key = e.getKey();
			float value = e.getValue();

			if (this.containsKey(key)) {
				this.put(key, this.get(key) + value);
			} else {
				this.put(key, value);
			}
		}
	}

	/**
	 * Computes the dot product of this map with another map.
	 * 
	 * @param m
	 *            the other map
	 */
	public int dot(Int2FloatOpenHashMapWritable m) {
		int s = 0;

		for (Int2FloatMap.Entry e : m.int2FloatEntrySet()) {
			int key = e.getKey();

			if (this.containsKey(key)) {
				s += this.get(key) * e.getValue();
			}
		}

		return s;
	}

	/**
	 * Increments the key. If the key does not exist in the map, its value is
	 * set to one.
	 * 
	 * @param key
	 *            key to increment
	 */
	public void increment(int key) {
		if (this.containsKey(key)) {
			this.put(key, this.get(key) + 1);
		} else {
			this.put(key, 1);
		}
	}
}
