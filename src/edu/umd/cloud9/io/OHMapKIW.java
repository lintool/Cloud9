package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.util.MapKI;
import edu.umd.cloud9.util.OHMapKI;

/**
 * <p>
 * Writable representing a map where the values are integers.
 * </p>
 * 
 * @param <K>
 *            type of key
 */
public class OHMapKIW<K extends WritableComparable> extends OHMapKI<K>
		implements Writable {

	private static final long serialVersionUID = 295863243L;

	/**
	 * Creates a MapKeyToIntWritable object.
	 */
	public OHMapKIW() {
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
				Integer s = in.readInt();
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
		Set<MapKI.Entry<K>> entries = entrySet();
		MapKI.Entry<K> first = entries.iterator().next();
		K objK = first.getKey();
		out.writeUTF(objK.getClass().getCanonicalName());

		// Then write out each key/value pair
		for (MapKI.Entry<K> e : entrySet()) {
			e.getKey().write(out);
			out.writeInt(e.getValue());
		}
	}

	public static <T extends WritableComparable> OHMapKIW<T> create(DataInput in)
			throws IOException {
		OHMapKIW<T> m = new OHMapKIW<T>();
		m.readFields(in);

		return m;
	}

}
