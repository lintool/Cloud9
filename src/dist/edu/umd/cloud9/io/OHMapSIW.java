package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.util.MapKI;
import edu.umd.cloud9.util.OHMapKI;

/**
 * Writable representing a map where keys are Strings and values are ints. This
 * class is specialized for String objects to avoid the overhead that comes with
 * wrapping Strings inside <code>Text</code> objects.
 * 
 * @author Jimmy Lin
 */
public class OHMapSIW extends OHMapKI<String> implements Writable {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a <code>OHMapSIW</code> object.
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

	/**
	 * Creates a <code>OHMapSIW</code> object from a <code>DataInput</code>.
	 * 
	 * @param in
	 *            <code>DataInput</code> for reading the serialized
	 *            representation
	 * @return a newly-created <code>OHMapSIW</code> object
	 * @throws IOException
	 */
	public static OHMapSIW create(DataInput in) throws IOException {
		OHMapSIW m = new OHMapSIW();
		m.readFields(in);

		return m;
	}
}
