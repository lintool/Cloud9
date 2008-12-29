package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.util.HashMapFloat;
import edu.umd.cloud9.util.MapFloat;

/**
 * <p>
 * Writable representing a feature vector of float values. This generic class,
 * based on the Java {@link HashMap}, supports the use of any class for the
 * features (i.e., component of the vector), but all values are floats.
 * </p>
 * 
 * @param <K>
 *            type of feature
 */
public class MapKeyToFloatWritable<K extends WritableComparable> extends HashMapFloat<K> implements
		Writable {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a VectorFloat object.
	 */
	public MapKeyToFloatWritable() {
		super();
	}

	/**
	 * Deserializes the vector.
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
	 * Serializes the vector.
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
		Set<MapFloat.Entry<K>> entries = entrySet();
		MapFloat.Entry<K> first = entries.iterator().next();
		K objK = first.getKey();
		out.writeUTF(objK.getClass().getCanonicalName());

		// Then write out each key/value pair
		for (MapFloat.Entry<K> e : entrySet()) {
			e.getKey().write(out);
			out.writeFloat(e.getValue());
		}
	}

	/**
	 * Adds another vector to this vector, based on feature-wise addition.
	 * 
	 * @param v
	 *            vector to add
	 */
	public void plus(MapKeyToFloatWritable<K> v) {
		for (MapFloat.Entry<K> e : v.entrySet()) {
			K key = e.getKey();

			if (this.containsKey(key)) {
				this.put(key, this.get(key) + e.getValue());
			} else {
				this.put(key, e.getValue());
			}
		}
	}

	/**
	 * Computes the dot product between this vector and another vector.
	 * 
	 * @param v
	 *            the other vector
	 */
	public float dot(MapKeyToFloatWritable<K> v) {
		float s = 0.0f;

		for (MapFloat.Entry<K> e : v.entrySet()) {
			K key = e.getKey();

			if (this.containsKey(key)) {
				s += this.get(key) * e.getValue();
			}
		}

		return s;
	}

	/**
	 * Computes the length of this vector.
	 * 
	 * @return length of this vector
	 */
	public float length() {
		float s = 0.0f;

		for (MapFloat.Entry<K> e : this.entrySet()) {
			s += e.getValue() * e.getValue();
		}

		return (float) Math.sqrt(s);
	}

	/**
	 * Normalizes this vector to a unit-length vector.
	 */
	public void normalize() {
		float l = this.length();

		for (K f : this.keySet()) {
			this.put(f, this.get(f) / l);
		}

	}

	/**
	 * Returns feature-value entries sorted by descending value. Ties broken by
	 * the natural sort order of the feature.
	 * 
	 * @return feature-value entries sorted by descending value
	 */
	public SortedSet<MapFloat.Entry<K>> getEntriesSortedByValue() {
		SortedSet<MapFloat.Entry<K>> entries = new TreeSet<MapFloat.Entry<K>>(
				new Comparator<MapFloat.Entry<K>>() {
					@SuppressWarnings("unchecked")
					public int compare(MapFloat.Entry<K> e1, MapFloat.Entry<K> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		for (MapFloat.Entry<K> entry : this.entrySet()) {
			entries.add(entry);
		}

		return Collections.unmodifiableSortedSet(entries);
	}

	/**
	 * Returns top <i>n</i> feature-value entries sorted by descending value.
	 * Ties broken by the natural sort order of the feature.
	 * 
	 * @param n
	 *            number of entries to return
	 * @return top <i>n</i> feature-value entries sorted by descending value
	 */
	public SortedSet<MapFloat.Entry<K>> getEntriesSortedByValue(int n) {
		SortedSet<MapFloat.Entry<K>> entries = new TreeSet<MapFloat.Entry<K>>(
				new Comparator<MapFloat.Entry<K>>() {
					@SuppressWarnings("unchecked")
					public int compare(MapFloat.Entry<K> e1, MapFloat.Entry<K> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		int cnt = 0;
		for (MapFloat.Entry<K> entry : getEntriesSortedByValue()) {
			entries.add(entry);
			cnt++;
			if (cnt >= n)
				break;
		}

		return Collections.unmodifiableSortedSet(entries);
	}
}
