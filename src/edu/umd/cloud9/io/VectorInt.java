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
 * Writable representing a vector of integer values. This generic class, based
 * on the Java {@link HashMap}, supports the use of any class for the features
 * (i.e., component of the vector), but all values are integers.
 * </p>
 * 
 * @param <F>
 *            type of feature
 */
public class VectorInt<F extends WritableComparable> extends HashMap<F, Integer> implements
		Writable {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a VectorInt object.
	 */
	public VectorInt() {
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

		F objK;

		try {
			Class keyClass = Class.forName(keyClassName);
			for (int i = 0; i < numEntries; i++) {
				objK = (F) keyClass.newInstance();
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
		Set<Map.Entry<F, Integer>> entries = entrySet();
		Map.Entry<F, Integer> first = entries.iterator().next();
		F objK = first.getKey();
		out.writeUTF(objK.getClass().getCanonicalName());

		// Then write out each key/value pair
		for (Map.Entry<F, Integer> e : entrySet()) {
			e.getKey().write(out);
			out.writeInt(e.getValue());
		}
	}

	/**
	 * Sets the value of a feature.
	 * 
	 * @param f
	 *            the feature
	 * @param v
	 *            the value
	 */
	public void set(F f, int v) {
		super.put(f, v);
	}

	/**
	 * Returns the value of a feature.
	 * 
	 * @param f
	 *            the feature
	 * @return the value of the feature
	 */
	public int get(F f) {
		return super.get(f);
	}

	/**
	 * Adds another vector to this vector, based on feature-wise addition.
	 * 
	 * @param v
	 *            vector to add
	 */
	public void plus(VectorInt<F> v) {
		for (Map.Entry<F, Integer> e : v.entrySet()) {
			F key = e.getKey();

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
	public int dot(VectorInt<F> v) {
		int s = 0;

		for (Map.Entry<F, Integer> e : v.entrySet()) {
			F key = e.getKey();

			if (this.containsKey(key)) {
				s += this.get(key) * e.getValue();
			}
		}

		return s;
	}

	/**
	 * Returns feature-value entries sorted by descending value. Ties broken by
	 * the natural sort order of the feature.
	 * 
	 * @return feature-value entries sorted by descending value
	 */
	public SortedSet<Map.Entry<F, Integer>> getSortedEntries() {
		SortedSet<Map.Entry<F, Integer>> entries = new TreeSet<Map.Entry<F, Integer>>(
				new Comparator<Map.Entry<F, Integer>>() {
					@SuppressWarnings("unchecked")
					public int compare(Map.Entry<F, Integer> e1, Map.Entry<F, Integer> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		for (Map.Entry<F, Integer> entry : this.entrySet()) {
			entries.add(entry);
		}

		return Collections.unmodifiableSortedSet(entries);
	}

}
