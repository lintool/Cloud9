package edu.umd.cloud9.io.map;

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

	private static boolean LAZY_DECODE = false;

	private int numEntries = 0;
	private int[] keys = null;
	private float[] values = null;

	/**
	 * Creates an <code>Int2FloatOpenHashMapWritable</code> object.
	 */
	public Int2FloatOpenHashMapWritable() {
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

		if (LAZY_DECODE) {
			// Lazy initialization; read into arrays.
			keys = new int[numEntries];
			values = new float[numEntries];

			for (int i = 0; i < numEntries; i++) {
				keys[i] = in.readInt();
				values[i] = in.readFloat();
			}
		} else {
      // Normal initialization; populate the map.
			for (int i = 0; i < numEntries; i++) {
				super.put(in.readInt(), in.readFloat());
			}
		}
	}

	/**
	 * In lazy decoding mode, populates the map with deserialized data.
	 * Otherwise, does nothing.
	 */
	public void decode() throws IOException {
		if (keys == null)
			return;

		for (int i = 0; i < keys.length; i++) {
			put(keys[i], values[i]);
		}

		keys = null;
	}

	public boolean hasBeenDecoded() {
		return keys == null;
	}

	/**
	 * Serializes the map.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		// Check to see if we're in lazy decode mode, and this object hasn't
		// been decoded yet.
		if (keys == null) {
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
			out.writeInt(numEntries);
			for (int i = 0; i < numEntries; i++) {
				out.writeInt(keys[i]);
				out.writeFloat(values[i]);
			}
		}
	}

  /**
   * Serializes this object to a byte array.
   *
   * @return byte array representing the serialized representation
   * @throws IOException
   */
	public byte[] serialize() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		write(dataOut);

		return bytesOut.toByteArray();
	}

  /**
   * Creates object from serialized representation.
   *
   * @param in source of serialized representation
   * @return newly-created object
   * @throws IOException
   */
	public static Int2FloatOpenHashMapWritable create(DataInput in) throws IOException {
		Int2FloatOpenHashMapWritable m = new Int2FloatOpenHashMapWritable();
		m.readFields(in);

		return m;
	}

  /**
   * Creates object from serialized representation.
   *
   * @param bytes source of serialized representation
   * @return newly-created object
   * @throws IOException
   */
	public static Int2FloatOpenHashMapWritable create(byte[] bytes) throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	/**
	 * Adds values of keys from another map to this map.
	 *
	 * @param m the other map
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
	 * @param m the other map
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
   * @param key key to increment
   */
  public void increment(int key) {
    increment(key, 1.0f);
  }

 /**
   * Increments the key. If the key does not exist in the map, its value is
   * set to one.
   *
   * @param key key to increment
   * @param n amount to increment
   */
  public void increment(int key, float n) {
    if (this.containsKey(key)) {
      this.put(key, this.get(key) + n);
    } else {
      this.put(key, n);
    }
  }

	/**
	 * Sets the lazy decoding flag.
	 *
	 * @param b the value of the lazy decoding flag
	 */
	public static void setLazyDecodeFlag(boolean b) {
		LAZY_DECODE = b;
	}

	/**
	 * Returns the value of the lazy decoding flag
	 *
	 * @return the value of the lazy decoding flag
	 */
	public static boolean getLazyDecodeFlag() {
		return LAZY_DECODE;
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
	 * In lazy decoding mode, adds values from keys of another map to this map.
	 * This map must have already been decoded, but the other map must not have
	 * been already decoded.
	 *
	 * @param m the other map
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
	 * @param k number of entries to return
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
