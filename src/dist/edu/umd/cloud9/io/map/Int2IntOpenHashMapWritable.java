package edu.umd.cloud9.io.map;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

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

public class Int2IntOpenHashMapWritable extends Int2IntOpenHashMap implements Writable {
	private static final long serialVersionUID = 1255879065743242L;

	private static boolean LAZY_DECODE = false;

	private int numEntries = 0;
	private int[] keys = null;
	private int[] values = null;

	/**
	 * Creates an <code>Int2IntOpenHashMapWritable</code> object.
	 */
	public Int2IntOpenHashMapWritable() {
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
			values = new int[numEntries];

			for (int i = 0; i < numEntries; i++) {
				keys[i] = in.readInt();
				values[i] = in.readInt();
			}
		} else {
			// Normal initialization; populate the map.
			for (int i = 0; i < numEntries; i++) {
				put(in.readInt(), in.readInt());
			}
		}
	}

	/**
	 * In lazy decoding mode, populates the map with deserialized data.
	 * Otherwise, does nothing.
	 */
	public void decode() {
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
			for (Int2IntMap.Entry e : int2IntEntrySet()) {
				out.writeInt(e.getIntKey());
				out.writeInt(e.getIntValue());
			}
		} else {
			out.writeInt(numEntries);
			for (int i = 0; i < numEntries; i++) {
				out.writeInt(keys[i]);
				out.writeInt(values[i]);
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
  public static Int2IntOpenHashMapWritable create(DataInput in) throws IOException {
    Int2IntOpenHashMapWritable m = new Int2IntOpenHashMapWritable();
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
	public static Int2IntOpenHashMapWritable create(byte[] bytes) throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	/**
	 * Adds values of keys from another map to this map.
	 * 
	 * @param m the other map
	 */
	public void plus(Int2IntOpenHashMapWritable m) {
		for (Int2IntMap.Entry e : m.int2IntEntrySet()) {
			int key = e.getIntKey();
			int value = e.getIntValue();

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
	public int dot(Int2IntOpenHashMapWritable m) {
		int s = 0;

		for (Int2IntMap.Entry e : m.int2IntEntrySet()) {
			int key = e.getIntKey();

			if (this.containsKey(key)) {
				s += this.get(key) * e.getIntValue();
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
	  increment(key, 1);
	}

 /**
   * Increments the key. If the key does not exist in the map, its value is
   * set to one.
   *
   * @param key key to increment
   * @param n amount to increment
   */
  public void increment(int key, int n) {
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
	public int[] getValues() {
		return values;
	}

	/**
	 * In lazy decoding mode, adds values from keys of another map to this map.
	 * This map must have already been decoded, but the other map must not have
	 * been already decoded.
	 *
	 * @param m the other map
	 */
	public void lazyplus(Int2IntOpenHashMapWritable m) {
		int[] k = m.getKeys();
		int[] v = m.getValues();

		for (int i = 0; i < k.length; i++) {
			if (this.containsKey(k[i])) {
				this.put(k[i], this.get(k[i]) + v[i]);
			} else {
				this.put(k[i], v[i]);
			}
		}
	}

	/**
	 * Returns entries sorted by descending value. Ties broken by the key.
	 *
	 * @return entries sorted by descending value
	 */
	public Int2IntMap.Entry[] getEntriesSortedByValue() {
		if (this.size() == 0)
			return null;

		Int2IntMap.Entry[] entries = new Int2IntMap.Entry[this.size()];
		entries = this.int2IntEntrySet().toArray(entries);

		// sort the entries
		Arrays.sort(entries, new Comparator<Int2IntMap.Entry>() {
			public int compare(Int2IntMap.Entry e1, Int2IntMap.Entry e2) {
				if (e1.getIntValue() > e2.getIntValue()) {
					return -1;
				} else if (e1.getIntValue() < e2.getIntValue()) {
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
	public Int2IntMap.Entry[] getEntriesSortedByValue(int k) {
		Int2IntMap.Entry[] entries = getEntriesSortedByValue();

		if (entries == null)
			return null;

		if (entries.length < k)
			return entries;

		return Arrays.copyOfRange(entries, 0, k);
	}
}
