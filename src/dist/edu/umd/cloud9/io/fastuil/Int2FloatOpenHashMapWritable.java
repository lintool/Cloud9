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
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.io.Writable;

public class Int2FloatOpenHashMapWritable extends Int2FloatOpenHashMap implements Writable {

	private static final long serialVersionUID = 674980125439241L;

	private static boolean sLazyDecode = false;

	private int mNumEntries = 0;
	private int[] mKeys = null;
	private float[] mValues = null;

	/**
	 * Creates an <code>Int2FloatOpenHashMapWritable</code> object.
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

		mNumEntries = in.readInt();
		if (mNumEntries == 0)
			return;

		if (sLazyDecode) {
			// Lazy initialization; read into arrays.
			mKeys = new int[mNumEntries];
			mValues = new float[mNumEntries];

			for (int i = 0; i < mNumEntries; i++) {
				mKeys[i] = in.readInt();
				mValues[i] = in.readFloat();
			}
		} else {
			for (int i = 0; i < mNumEntries; i++) {
				super.put(in.readInt(), in.readFloat());
			}
		}
	}

	/**
	 * In lazy decoding mode, populates the map with deserialized data.
	 * Otherwise, does nothing.
	 */
	public void decode() throws IOException {
		if (mKeys == null)
			return;

		for (int i = 0; i < mKeys.length; i++) {
			put(mKeys[i], mValues[i]);
		}

		mKeys = null;
	}

	public boolean hasBeenDecoded() {
		return mKeys == null;
	}

	/**
	 * Serializes the map.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		// Check to see if we're in lazy decode mode, and this object hasn't
		// been decoded yet.
		if (mKeys == null) {
			// Write out the number of entries in the map.
			out.writeInt(size());
			if (size() == 0)
				return;

			// Then write out each key/value pair.
			for (Int2FloatMap.Entry e : int2FloatEntrySet()) {
				out.writeInt(e.getKey());
				out.writeFloat(e.getValue());
			}
		} else {
			out.writeInt(mNumEntries);
			for (int i = 0; i < mNumEntries; i++) {
				out.writeInt(mKeys[i]);
				out.writeFloat(mValues[i]);
			}
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
	 * Creates an <code>Int2FloatOpenHashMapWritable</code> object from a
	 * <code>DataInput</code>.
	 * 
	 * @param in
	 *            <code>DataInput</code> for reading the serialized
	 *            representation
	 * @return a newly-created <code>Int2FloatOpenHashMapWritable</code> object
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
	 * Sets the lazy decoding flag.
	 * 
	 * @param b
	 *            the value of the lazy decoding flag
	 */
	public static void setLazyDecodeFlag(boolean b) {
		sLazyDecode = b;
	}

	/**
	 * Returns the value of the lazy decoding flag
	 * 
	 * @return the value of the lazy decoding flag
	 */
	public static boolean getLazyDecodeFlag() {
		return sLazyDecode;
	}

	/**
	 * In lazy decoding mode, returns an array of all the keys if the map hasn't
	 * been decoded yet. Otherwise, returns null.
	 * 
	 * @return an array of all the keys
	 */
	public int[] getKeys() {
		return mKeys;
	}

	/**
	 * In lazy decoding mode, returns an array of all the values if the map
	 * hasn't been decoded yet. Otherwise, returns null.
	 * 
	 * @return an array of all the values
	 */
	public float[] getValues() {
		return mValues;
	}

	/**
	 * In lazy decoding mode, adds values from keys of another map to this map.
	 * This map must have already been decoded, but the other map must not have
	 * been already decoded.
	 * 
	 * @param m
	 *            the other map
	 */
	public void lazyplus(Int2FloatOpenHashMapWritable m) {
		int[] keys = m.getKeys();
		float[] values = m.getValues();

		for (int i = 0; i < keys.length; i++) {
			if (this.containsKey(keys[i])) {
				this.put(keys[i], this.get(keys[i]) + values[i]);
			} else {
				this.put(keys[i], values[i]);
			}
		}
	}

	/**
	 * Returns entries sorted by descending value. Ties broken by the key.
	 * 
	 * @return entries sorted by descending value
	 */
	public Int2FloatMap.Entry[] getEntriesSortedByValue() {
		if (this.size() == 0)
			return null;

		Int2FloatMap.Entry[] entries = new Int2FloatMap.Entry[this.size()];
		entries = this.int2FloatEntrySet().toArray(entries);

		// Sort the entries.
		Arrays.sort(entries, new Comparator<Int2FloatMap.Entry>() {
			public int compare(Int2FloatMap.Entry e1, Int2FloatMap.Entry e2) {
				if (e1.getFloatValue() > e2.getFloatValue()) {
					return -1;
				} else if (e1.getFloatValue() < e2.getFloatValue()) {
					return 1;
				}

				if (e1.getIntKey() == e2.getIntKey())
					return 0;

				return e1.getIntKey() > e2.getIntKey() ? 1 : -1;
			}
		});

		return entries;
	}

	/**
	 * Returns top <i>k</i> entries sorted by descending value. Ties broken by
	 * the key.
	 * 
	 * @param k
	 *            number of entries to return
	 * @return top <i>k</i> entries sorted by descending value
	 */
	public Int2FloatMap.Entry[] getEntriesSortedByValue(int k) {
		Int2FloatMap.Entry[] entries = getEntriesSortedByValue();

		if (entries == null)
			return null;

		if (entries.length < k)
			return entries;

		return Arrays.copyOfRange(entries, 0, k);
	}
}
