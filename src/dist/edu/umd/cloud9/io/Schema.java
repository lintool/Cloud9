/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.io;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

/**
 * <p>
 * Description of a Tuple's structure. The Schema class keeps track of column
 * names, data types, and default values. The following code fragment
 * illustrates the use of this class:
 * </p>
 * 
 * <pre>
 * public static final Schema MYSCHEMA = new Schema();
 * static {
 * 	MYSCHEMA.addField(&quot;token&quot;, String.class, &quot;&quot;);
 * 	MYSCHEMA.addField(&quot;int&quot;, Integer.class, new Integer(1));
 * }
 * </pre>
 * 
 * <p>
 * The following field types are allowed:
 * </p>
 * 
 * <ul>
 * <li>Basic Java primitives: Boolean, Integer, Long, Float, Double, String</li>
 * <li>Classes that implement Writable</li>
 * </ul>
 * 
 * <p>
 * Schema instances can be locked to prevent further changes. Any attempt to
 * alter a locked Schema will result in a runtime exception being thrown. If a
 * Schema is not locked, callers are free to add new fields and edit default
 * values.
 * </p>
 * 
 * <p>
 * New Tuple instances can be created directly from Schema objects through the
 * use of the {@link #instantiate()} method. A call to that method implicitly
 * locks the Schema.
 * </p>
 * 
 * <p>
 * <b>Acknowledgments:</b> much of this code was adapted from the <a
 * href="http://prefuse.org/">Prefuse Visualization Toolkit</a>.
 * </p>
 * 
 * @author Jimmy Lin
 */
public class Schema implements Cloneable {

	private String[] mFieldNames;
	private Class<?>[] mFieldTypes;
	private Object[] mDefaultValues;
	private Map<String, Integer> mFieldLookup;
	private int mFieldCount;
	private boolean mLocked;

	// ------------------------------------------------------------------------
	// Constructors

	/**
	 * Creates a new empty Schema.
	 */
	public Schema() {
		this(10);
	}

	/**
	 * Creates a new empty Schema with a starting capacity for a given number of
	 * fields.
	 * 
	 * @param n
	 *            the number of columns in this schema
	 */
	public Schema(int n) {
		mFieldNames = new String[n];
		mFieldTypes = new Class<?>[n];
		mDefaultValues = new Object[n];
		mFieldCount = 0;
		mLocked = false;
	}

	/**
	 * Create a new Schema consisting of the given field names and types.
	 * 
	 * @param names
	 *            the field names
	 * @param types
	 *            the field types (as Class instances)
	 */
	public Schema(String[] names, Class<?>[] types) {
		this(names.length);

		// check the schema validity
		if (names.length != types.length) {
			throw new IllegalArgumentException("Input arrays should be the same length");
		}
		for (int i = 0; i < names.length; ++i) {
			addField(names[i], types[i], null);
		}
	}

	/**
	 * Create a new Schema consisting of the given field names, types, and
	 * default field values.
	 * 
	 * @param names
	 *            the field names
	 * @param types
	 *            the field types (as Class instances)
	 * @param defaults
	 *            the default values for each field
	 */
	public Schema(String[] names, Class<?>[] types, Object[] defaults) {
		this(names.length);

		// check the schema validity
		if (names.length != types.length || types.length != defaults.length) {
			throw new IllegalArgumentException("Input arrays should be the same length");
		}
		for (int i = 0; i < names.length; ++i) {
			addField(names[i], types[i], defaults[i]);
		}
	}

	/**
	 * Creates a copy of this Schema. Cloned copies of a locked Schema will not
	 * inherit the locked status.
	 * 
	 * @see java.lang.Object#clone()
	 */
	public Object clone() {
		Schema s = new Schema(mFieldCount);
		for (int i = 0; i < mFieldCount; ++i) {
			s.addField(mFieldNames[i], mFieldTypes[i], mDefaultValues[i]);
		}
		return s;
	}

	/**
	 * Lazily construct the lookup table for this schema. Used to accelerate
	 * name-based lookups of schema information.
	 */
	protected void initLookup() {
		mFieldLookup = new HashMap<String, Integer>();
		for (int i = 0; i < mFieldNames.length; ++i) {
			mFieldLookup.put(mFieldNames[i], new Integer(i));
		}
	}

	// ------------------------------------------------------------------------
	// Accessors / Mutators

	/**
	 * Locks the Schema, preventing any additional changes. Locked Schemas
	 * cannot be unlocked! Cloned copies of a locked schema will not inherit
	 * this locked status.
	 * 
	 * @return a reference to this schema
	 */
	public Schema lockSchema() {
		mLocked = true;
		return this;
	}

	/**
	 * Checks if this schema is locked. Locked Schemas can not be edited.
	 * 
	 * @return true if this Schema is locked, false otherwise
	 */
	public boolean isLocked() {
		return mLocked;
	}

	/**
	 * Adds a field to this Schema.
	 * 
	 * @param name
	 *            the field name
	 * @param type
	 *            the field type (as a Class instance)
	 * @throws IllegalArgumentException
	 *             if either name or type are null or the name already exists in
	 *             this schema.
	 */
	public void addField(String name, Class<?> type) {
		addField(name, type, null);
	}

	/**
	 * Adds a field to this schema.
	 * 
	 * @param name
	 *            the field name
	 * @param type
	 *            the field type (as a Class instance)
	 * @throws IllegalArgumentException
	 *             if either name or type are null or the name already exists in
	 *             this schema.
	 */
	public void addField(String name, Class<?> type, Object defaultValue) {
		if (!(type == Integer.class || type == Boolean.class || type == Long.class
				|| type == Float.class || type == Double.class || type == String.class || (!type
				.isInterface() && Writable.class.isAssignableFrom(type)))) {
			throw new SchemaException("Illegal field type: " + type.getCanonicalName());
		}

		// check lock status
		if (mLocked) {
			throw new IllegalStateException("Can not add column to a locked Schema.");
		}
		// check for validity
		if (name == null) {
			throw new IllegalArgumentException("Null column names are not allowed.");
		}
		if (type == null) {
			throw new IllegalArgumentException("Null column types are not allowed.");
		}
		for (int i = 0; i < mFieldCount; ++i) {
			if (mFieldNames[i].equals(name)) {
				throw new IllegalArgumentException("Duplicate column names are not allowed: "
						+ mFieldNames[i]);
			}
		}

		// resize if necessary
		if (mFieldNames.length == mFieldCount) {
			int capacity = (3 * mFieldNames.length) / 2 + 1;
			String[] names = new String[capacity];
			Class<?>[] types = new Class[capacity];
			Object[] dflts = new Object[capacity];
			System.arraycopy(mFieldNames, 0, names, 0, mFieldCount);
			System.arraycopy(mFieldTypes, 0, types, 0, mFieldCount);
			System.arraycopy(mDefaultValues, 0, dflts, 0, mFieldCount);
			mFieldNames = names;
			mFieldTypes = types;
			mDefaultValues = dflts;
		}

		mFieldNames[mFieldCount] = name;
		mFieldTypes[mFieldCount] = type;
		mDefaultValues[mFieldCount] = defaultValue;

		if (mFieldLookup != null)
			mFieldLookup.put(name, new Integer(mFieldCount));

		mFieldCount++;
	}

	/**
	 * Returns the number of fields in this Schema.
	 * 
	 * @return the number of fields in this Schema
	 */
	public int getFieldCount() {
		return mFieldCount;
	}

	/**
	 * Returns the name of the field at the given position.
	 * 
	 * @param index
	 *            the field index
	 * @return the field name
	 */
	public String getFieldName(int index) {
		return mFieldNames[index];
	}

	/**
	 * Returns the position of a field given its name.
	 * 
	 * @param field
	 *            the field name
	 * @return the field position index
	 */
	public int getFieldIndex(String field) {
		if (mFieldLookup == null)
			initLookup();

		Integer idx = (Integer) mFieldLookup.get(field);
		return (idx == null ? -1 : idx.intValue());
	}

	/**
	 * Returns the type of the field at the given position.
	 * 
	 * @param index
	 *            the column index
	 * @return the column type
	 */
	public Class<?> getFieldType(int index) {
		return mFieldTypes[index];
	}

	/**
	 * Returns the type of the field given its name.
	 * 
	 * @param field
	 *            the field name
	 * @return the field type
	 */
	public Class<?> getFieldType(String field) {
		int idx = getFieldIndex(field);
		return (idx < 0 ? null : mFieldTypes[idx]);
	}

	/**
	 * Returns the default value of the field at the given position.
	 * 
	 * @param index
	 *            the field index
	 * @return the field's default value
	 */
	public Object getDefault(int index) {
		return mDefaultValues[index];
	}

	/**
	 * Returns the default value of the field with the given name.
	 * 
	 * @param field
	 *            the field name
	 * @return the field's default value
	 */
	public Object getDefault(String field) {
		int idx = getFieldIndex(field);
		return (idx < 0 ? null : mDefaultValues[idx]);
	}

	/**
	 * Sets the default value for the given field.
	 * 
	 * @param index
	 *            the index position of the field to set the default for
	 * @param val
	 *            the new default value
	 */
	public void setDefault(int index, Object val) {
		// check lock status
		if (mLocked) {
			throw new IllegalStateException("Can not update default values of a locked Schema.");
		}
		mDefaultValues[index] = val;
	}

	/**
	 * Sets the default value for the given field.
	 * 
	 * @param field
	 *            the name of field to set the default for
	 * @param val
	 *            the new default value
	 */
	public void setDefault(String field, Object val) {
		// check lock status
		if (mLocked) {
			throw new IllegalStateException("Can not update default values of a locked Schema.");
		}
		int idx = getFieldIndex(field);
		mDefaultValues[idx] = val;
	}

	/**
	 * Sets the default value for the given field as an <code>int</code>.
	 * 
	 * @param field
	 *            the name of field to set the default for
	 * @param val
	 *            the new default value
	 */
	public void setDefault(String field, int val) {
		setDefault(field, new Integer(val));
	}

	/**
	 * Set the default value for the given field as a <code>long</code>.
	 * 
	 * @param field
	 *            the name of field to set the default for
	 * @param val
	 *            the new default value
	 */
	public void setDefault(String field, long val) {
		setDefault(field, new Long(val));
	}

	/**
	 * Set the default value for the given field as a <code>float</code>.
	 * 
	 * @param field
	 *            the name of field to set the default for
	 * @param val
	 *            the new default value
	 */
	public void setDefault(String field, float val) {
		setDefault(field, new Float(val));
	}

	/**
	 * Set the default value for the given field as a <code>double</code>.
	 * 
	 * @param field
	 *            the name of field to set the default for
	 * @param val
	 *            the new default value
	 */
	public void setDefault(String field, double val) {
		setDefault(field, new Double(val));
	}

	/**
	 * Set the default value for the given field as a <code>boolean</code>.
	 * 
	 * @param field
	 *            the name of field to set the default for
	 * @param val
	 *            the new default value
	 */
	public void setDefault(String field, boolean val) {
		setDefault(field, val ? Boolean.TRUE : Boolean.FALSE);
	}

	// ------------------------------------------------------------------------
	// Comparison and Hashing

	/**
	 * Compares this Schema with another one for equality.
	 */
	public boolean equals(Object o) {
		if (!(o instanceof Schema))
			return false;

		Schema s = (Schema) o;
		if (mFieldCount != s.getFieldCount())
			return false;

		for (int i = 0; i < mFieldCount; ++i) {
			if (!(mFieldNames[i].equals(s.getFieldName(i))
					&& mFieldTypes[i].equals(s.getFieldType(i)) && mDefaultValues[i].equals(s
					.getDefault(i)))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Computes a hashcode for this schema.
	 */
	public int hashCode() {
		int hashcode = 0;
		for (int i = 0; i < mFieldCount; ++i) {
			int idx = i + 1;
			int code = idx * mFieldNames[i].hashCode();
			code ^= idx * mFieldTypes[i].hashCode();
			if (mDefaultValues[i] != null)
				code ^= mDefaultValues[i].hashCode();
			hashcode ^= code;
		}
		return hashcode;
	}

	/**
	 * Returns a descriptive String for this schema.
	 */
	public String toString() {
		StringBuffer sbuf = new StringBuffer();
		sbuf.append("Schema[");
		for (int i = 0; i < mFieldCount; ++i) {
			if (i > 0)
				sbuf.append(' ');
			sbuf.append('(').append(mFieldNames[i]).append(", ");
			sbuf.append(mFieldTypes[i].getName()).append(", ");
			sbuf.append(mDefaultValues[i]).append(')');
		}
		sbuf.append(']');
		return sbuf.toString();
	}

	// ------------------------------------------------------------------------
	// Tuple Operations

	/**
	 * Instantiate a new Tuple instance with this Schema. Fields of the newly
	 * instantiated Tuple are set to default value.
	 * 
	 * @return a new Tuple with this Schema
	 */
	public Tuple instantiate() {
		lockSchema();

		Object[] objects = new Object[mFieldCount];
		System.arraycopy(mDefaultValues, 0, objects, 0, mFieldCount);

		String[] symbols = new String[mFieldCount];

		String[] fields = new String[mFieldCount];
		System.arraycopy(mFieldNames, 0, fields, 0, mFieldCount);

		Class<?>[] types = new Class<?>[mFieldCount];
		System.arraycopy(mFieldTypes, 0, types, 0, mFieldCount);

		return new Tuple(objects, symbols, fields, types);
	}

	/**
	 * Instantiate a new Tuple instance with this Schema.
	 * 
	 * @param objects
	 *            values of each field
	 * @return a new Tuple with this Schema
	 */
	public Tuple instantiate(Object... objects) {
		lockSchema();

		String[] symbols = new String[mFieldCount];

		String[] fields = new String[mFieldCount];
		System.arraycopy(mFieldNames, 0, fields, 0, mFieldCount);

		Class<?>[] types = new Class[mFieldCount];
		System.arraycopy(mFieldTypes, 0, types, 0, mFieldCount);

		return new Tuple(objects, symbols, fields, types);
	}

} // end of class Schema
