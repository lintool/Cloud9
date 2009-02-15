package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.util.MapKI;
import edu.umd.cloud9.util.OHMapKI;

/**
 * <p>
 * Writable representing a map where the values are floats.
 * </p>
 * 
 * @param <K>
 *            type of key
 */
public class OHMapSIW extends OHMapKI<String> implements Writable {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a MapKeyToFloatWritable object.
	 */
	public OHMapSIW() {
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
			put(k, v);
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
		for (MapKI.Entry<String> e : entrySet()) {
			out.writeUTF(e.getKey());
			out.writeInt(e.getValue());
		}
	}

	public static OHMapSIW create(DataInput in) throws IOException {
		OHMapSIW m = new OHMapSIW();
		m.readFields(in);

		return m;
	}
}
