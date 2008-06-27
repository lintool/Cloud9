package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * <p>
 * Serializable object in Hadoop framework that represents a vector of float
 * values.
 * </p>
 */
public class VectorFloat<K extends WritableComparable> extends HashMap<K, Float> implements
		Writable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a HashMapWritable object.
	 */
	public VectorFloat() {
		super();
	}

	/**
	 * Deserializes the array.
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
	 * Serializes this array.
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
		// assuming that data is homogeneuos (i.e., all entries have same types)
		Set<Map.Entry<K, Float>> entries = entrySet();
		Map.Entry<K, Float> first = entries.iterator().next();
		K objK = first.getKey();
		out.writeUTF(objK.getClass().getCanonicalName());

		// Then write out each key/value pair
		for (Map.Entry<K, Float> e : entrySet()) {
			e.getKey().write(out);
			out.writeFloat(e.getValue());
		}
	}


	public void set(K k, float v) {
		super.put(k, v);
	}
	
	public float get(K v) {
		return super.get(v);
	}
	
	public void plus(VectorFloat<K> map) {
		for (Map.Entry<K, Float> e : map.entrySet()) {
			K key = e.getKey();

			if (this.containsKey(key)) {
				this.put(key, this.get(key) + e.getValue());
			} else {
				this.put(key, e.getValue());
			}
		}
	}

	public SortedSet<Map.Entry<K, Float>> getSortedEntries() {
		SortedSet<Map.Entry<K, Float>> entries = new TreeSet<Map.Entry<K, Float>>(
				new Comparator<Map.Entry<K, Float>>() {
					@SuppressWarnings("unchecked")
					public int compare(Map.Entry<K, Float> e1, Map.Entry<K, Float> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		for (Map.Entry<K, Float> entry : this.entrySet()) {
			entries.add(entry);
		}

		return Collections.unmodifiableSortedSet(entries);
	}

	public SortedSet<Map.Entry<K, Float>> getSortedEntries(int n) {
		SortedSet<Map.Entry<K, Float>> entries = new TreeSet<Map.Entry<K, Float>>(
				new Comparator<Map.Entry<K, Float>>() {
					@SuppressWarnings("unchecked")
					public int compare(Map.Entry<K, Float> e1, Map.Entry<K, Float> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		for (Map.Entry<K, Float> entry : this.entrySet()) {
			entries.add(entry);
		}

		return Collections.unmodifiableSortedSet(entries);
	}
}
