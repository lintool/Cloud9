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

		int numEntries = in.readInt();
		if (numEntries == 0)
			return;

		for (int i = 0; i < numEntries; i++) {
			// int k = WritableUtils.readVInt(in);
			int k = in.readInt();
			float s = in.readFloat();
			put(k, s);
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

}
