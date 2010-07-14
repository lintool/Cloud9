package edu.umd.cloud9.io.fastuil;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class String2IntOpenHashMapWritable extends Object2IntOpenHashMap<String> implements
		Writable {

	private static final long serialVersionUID = 276091731841463L;
	
	/**
	 * Creates a <code>String2IntOpenHashMapWritable</code> object.
	 */
	public String2IntOpenHashMapWritable() {
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
			String k = in.readUTF();
			int v = in.readInt();
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
		for (Object2IntMap.Entry<String> e : object2IntEntrySet()) {
			out.writeUTF(e.getKey());
			out.writeInt(e.getValue());
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
	public static String2IntOpenHashMapWritable create(DataInput in) throws IOException {
		String2IntOpenHashMapWritable m = new String2IntOpenHashMapWritable();
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
	public static String2IntOpenHashMapWritable create(byte[] bytes) throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	/**
	 * Adds values of keys from another map to this map.
	 * 
	 * @param m
	 *            the other map
	 */
	public void plus(String2IntOpenHashMapWritable m) {
		for (Object2IntMap.Entry<String> e : m.object2IntEntrySet()) {
			String key = e.getKey();

			if (this.containsKey(key)) {
				this.put(key, this.get(key) + e.getValue());
			} else {
				this.put(key, e.getValue());
			}
		}
	}

	/**
	 * Computes the dot product of this map with another map.
	 * 
	 * @param m
	 *            the other map
	 */
	public int dot(String2IntOpenHashMapWritable m) {
		int s = 0;

		for (Object2IntMap.Entry<String> e : m.object2IntEntrySet()) {
			String key = e.getKey();

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
	public void increment(String key) {
		if (this.containsKey(key)) {
			this.put(key, this.get(key) + 1);
		} else {
			this.put(key, 1);
		}
	}
}
