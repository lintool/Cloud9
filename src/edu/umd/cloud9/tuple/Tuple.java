package edu.umd.cloud9.tuple;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Tuple implements WritableComparable {

	protected static final int INT = -1;
	protected static final int BOOLEAN = -2;
	protected static final int LONG = -3;
	protected static final int FLOAT = -4;
	protected static final int DOUBLE = -5;
	protected static final int STRING = -6;

	private Object[] mObjects;
	private String[] mFields;
	private Class[] mTypes;

	private byte[] mBytes = null;

	private Map<String, Integer> mFieldLookup = null;

	protected Tuple(Object[] objs, String[] fields, Class[] types) {
		mObjects = objs;
		mFields = fields;
		mTypes = types;
	}

	// WritableComparable needs empty constructor
	public Tuple() {
	}

	public static Tuple createFrom(DataInput in) throws IOException {
		Tuple tuple = new Tuple();
		tuple.readFields(in);

		return tuple;
	}

	public void set(int i, Object o) {
		mObjects[i] = o;

		// invalidate serialized representation
		mBytes = null;
	}

	public void set(String field, Object o) {
		if (mFieldLookup == null)
			initLookup();

		mObjects[mFieldLookup.get(field)] = o;

		// invalidate serialized representation
		mBytes = null;
	}
	
	public Object get(int i) {
		return mObjects[i];
	}

	public Object get(String field) {
		if (mFieldLookup == null)
			initLookup();

		return mObjects[mFieldLookup.get(field)];
	}

	/**
	 * Lazily construct the lookup table for this schema. Used to accelerate
	 * name-based lookups of schema information.
	 */
	private void initLookup() {
		mFieldLookup = new HashMap<String, Integer>();
		for (int i = 0; i < mFields.length; ++i) {
			mFieldLookup.put(mFields[i], new Integer(i));
		}
	}

	public byte[] getBytes() {
		if (mBytes == null)
			generateByteRepresentation();

		return mBytes;
	}

	private void generateByteRepresentation() {
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(byteStream);
		try {
			for (int i = 0; i < mFields.length; i++) {
				if (mTypes[i] == Integer.class) {
					out.writeInt((Integer) mObjects[i]);
				} else if (mTypes[i] == Boolean.class) {
					out.writeBoolean((Boolean) mObjects[i]);
				} else if (mTypes[i] == Long.class) {
					out.writeLong((Long) mObjects[i]);
				} else if (mTypes[i] == Float.class) {
					out.writeFloat((Float) mObjects[i]);
				} else if (mTypes[i] == Double.class) {
					out.writeDouble((Double) mObjects[i]);
				} else {
					out.writeUTF(mObjects[i].toString());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		mBytes = byteStream.toByteArray();
	}

	public void readFields(DataInput in) throws IOException {

		int numFields = in.readInt();

		mObjects = new Object[numFields];
		mFields = new String[numFields];
		mTypes = new Class[numFields];

		for (int i = 0; i < numFields; i++) {
			mFields[i] = in.readUTF();
		}

		for (int i = 0; i < numFields; i++) {
			int type = in.readInt();

			if (type == INT) {
				mTypes[i] = Integer.class;
				mObjects[i] = in.readInt();
			} else if (type == BOOLEAN) {
				mTypes[i] = Boolean.class;
				mObjects[i] = in.readBoolean();
			} else if (type == LONG) {
				mTypes[i] = Long.class;
				mObjects[i] = in.readLong();
			} else if (type == FLOAT) {
				mTypes[i] = Float.class;
				mObjects[i] = in.readFloat();
			} else if (type == DOUBLE) {
				mTypes[i] = Double.class;
				mObjects[i] = in.readDouble();
			} else {
				mTypes[i] = String.class;
				mObjects[i] = in.readUTF();
			}

		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(mFields.length);
		for (int i = 0; i < mFields.length; i++) {
			out.writeUTF(mFields[i]);
		}

		for (int i = 0; i < mFields.length; i++) {
			if (mTypes[i] == Integer.class) {
				out.writeInt(INT);
				out.writeInt((Integer) mObjects[i]);
			} else if (mTypes[i] == Boolean.class) {
				out.writeInt(BOOLEAN);
				out.writeBoolean((Boolean) mObjects[i]);
			} else if (mTypes[i] == Long.class) {
				out.writeInt(LONG);
				out.writeLong((Long) mObjects[i]);
			} else if (mTypes[i] == Float.class) {
				out.writeInt(FLOAT);
				out.writeFloat((Float) mObjects[i]);
			} else if (mTypes[i] == Double.class) {
				out.writeInt(DOUBLE);
				out.writeDouble((Double) mObjects[i]);
			} else {
				out.writeInt(STRING);
				out.writeUTF(mObjects[i].toString());
			}
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < mFields.length; i++) {
			if (i != 0)
				sb.append(", ");
			sb.append(mObjects[i]);
		}

		return "(" + sb.toString() + ")";
	}

	public int compareTo(Object obj) {
		byte[] thoseBytes = ((Tuple) obj).getBytes();
		byte[] theseBytes = this.getBytes();

		return WritableComparator.compareBytes(theseBytes, 0,
				theseBytes.length, thoseBytes, 0, thoseBytes.length);
	}

	public int hashCode() {
		if (mBytes == null)
			generateByteRepresentation();

		return WritableComparator.hashBytes(mBytes, mBytes.length);
	}

	/** A Comparator optimized for Tuple. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(Tuple.class);
		}

		/**
		 * Compare the buffers in serialized form.
		 */
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	// register this comparator
	static {
		WritableComparator.define(Tuple.class, new Comparator());
	}

}
