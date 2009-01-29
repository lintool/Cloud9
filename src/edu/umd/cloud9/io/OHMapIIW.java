package edu.umd.cloud9.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.util.MapII;
import edu.umd.cloud9.util.OHMapII;

/**
 * <p>
 * Writable representing a map where the values are floats.
 * </p>
 * 
 * @param <K>
 *            type of key
 */
public class OHMapIIW extends OHMapII implements Writable {

	private static boolean sLazyDecode = false;

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a MapKeyToFloatWritable object.
	 */
	public OHMapIIW() {
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
			values = new int[numEntries];

			for (int i = 0; i < numEntries; i++) {
				keys[i] = in.readInt();
				values[i] = in.readInt();
			}
		} else {
			// normal initialization; populate the map
			for (int i = 0; i < numEntries; i++) {
				put(in.readInt(), in.readInt());
			}
		}
	}

	int numEntries = 0;
	int[] keys = null;
	int[] values = null;

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

		for (MapII.Entry e : entrySet()) {
			out.writeInt(e.getKey());
			out.writeInt(e.getValue());
		}
	}

	public byte[] serialize() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		write(dataOut);

		return bytesOut.toByteArray();
	}

	public static OHMapIIW create(DataInput in) throws IOException {
		OHMapIIW m = new OHMapIIW();
		m.readFields(in);

		return m;
	}

	public static OHMapIIW create(byte[] bytes) throws IOException {
		return OHMapIIW.create(new DataInputStream(new ByteArrayInputStream(bytes)));
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

	public int[] getValues() {
		return values;
	}

	public void lazyplus(OHMapIIW m) {
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
}
