package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.util.MapIF;
import edu.umd.cloud9.util.OHMapIF;

/**
 * <p>
 * Writable representing a map where the values are floats.
 * </p>
 * 
 * @param <K>
 *            type of key
 */
public class OHMapIFW extends OHMapIF implements Writable {

	private static boolean sLazyDecode = false;

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a MapKeyToFloatWritable object.
	 */
	public OHMapIFW() {
		super();
	}

	/**
	 * Deserializes the map.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {

		this.clear();

		numEntries = in.readInt();
		if (numEntries == 0)
			return;

		if (sLazyDecode) {
			// lazy initialization; read into arrays
			keys = new int[numEntries];
			values = new float[numEntries];

			for (int i = 0; i < numEntries; i++) {
				keys[i] = in.readInt();
				values[i] = in.readFloat();
			}
		} else {
			// normal initialization; populate the map
			for (int i = 0; i < numEntries; i++) {
				put(in.readInt(), in.readFloat());
			}
		}

		// decode();
	}

	int numEntries = 0;
	int[] keys = null;
	float[] values = null;

	public void decode() throws IOException {
		if (keys == null)
			return;

		for (int i = 0; i < keys.length; i++) {
			put(keys[i], values[i]);
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

		for (MapIF.Entry e : entrySet()) {
			// WritableUtils.writeVInt(out, e.getKey());
			out.writeInt(e.getKey());
			out.writeFloat(e.getValue());
		}
	}

	public static OHMapIFW create(DataInput in) throws IOException {
		OHMapIFW m = new OHMapIFW();
		m.readFields(in);

		return m;
	}

	public static void setLazyDecodeFlag(boolean b) {
		sLazyDecode = b;
	}

	public static boolean getLazyDecodeFlag() {
		return sLazyDecode;
	}

	public int[] getKeys() {
		return keys;
	}

	public float[] getValues() {
		return values;
	}

	public void plus(OHMapIFW m) {
		if (!sLazyDecode) {
			super.plus(m);
		} else {
			int[] k = m.getKeys();
			float[] v = m.getValues();

			for (int i = 0; i < k.length; i++) {
				if (this.containsKey(k[i])) {
					this.put(k[i], this.get(k[i]) + v[i]);
				} else {
					this.put(k[i], v[i]);
				}
			}
		}
	}
}
