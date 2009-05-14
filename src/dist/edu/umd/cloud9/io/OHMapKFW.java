package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.util.MapKF;
import edu.umd.cloud9.util.OHMapKF;

/**
 * Writable representing a map where values are floats.
 * 
 * @param <K>
 *            type of key
 *            
 * @author Jimmy Lin
 */
public class OHMapKFW<K extends WritableComparable> extends OHMapKF<K> implements Writable {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a <code>OHMapKFW</code> object.
	 */
	public OHMapKFW() {
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

		String keyClassName = in.readUTF();

		K objK;
		try {
			Class keyClass = Class.forName(keyClassName);
			for (int i = 0; i < numEntries; i++) {
				objK = (K) keyClass.newInstance();
				objK.readFields(in);
				float s = in.readFloat();
				put(objK, s);
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
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

		// Write out the class names for keys and values
		// assuming that data is homogeneous (i.e., all entries have same types)
		Set<MapKF.Entry<K>> entries = entrySet();
		MapKF.Entry<K> first = entries.iterator().next();
		K objK = first.getKey();
		out.writeUTF(objK.getClass().getCanonicalName());

		// Then write out each key/value pair
		for (MapKF.Entry<K> e : entrySet()) {
			e.getKey().write(out);
			out.writeFloat(e.getValue());
		}
	}

	/**
	 * Creates a <code>OHMapKFW</code> object from a <code>DataInput</code>.
	 * 
	 * @param in
	 *            <code>DataInput</code> for reading the serialized
	 *            representation
	 * @return a newly-created <code>OHMapKFW</code> object
	 * @throws IOException
	 */
	public static <T extends WritableComparable> OHMapKFW<T> create(DataInput in)
			throws IOException {
		OHMapKFW<T> m = new OHMapKFW<T>();
		m.readFields(in);

		return m;
	}
}
