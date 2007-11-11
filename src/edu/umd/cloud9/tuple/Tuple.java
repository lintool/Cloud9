package edu.umd.cloud9.tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Tuple implements WritableComparable {

	protected static final byte SYMBOL = 0;
	protected static final byte INT = 1;
	protected static final byte BOOLEAN = 2;
	protected static final byte LONG = 3;
	protected static final byte FLOAT = 4;
	protected static final byte DOUBLE = 5;
	protected static final byte STRING = 6;
	protected static final byte WRITABLE = 7;

	private Object[] mObjects;
	private String[] mSymbols;
	private String[] mFields;
	private Class<?>[] mTypes;

	private byte[] mBytes = null;

	private Map<String, Integer> mFieldLookup = null;

	protected Tuple(Object[] objects, String[] symbols, String[] fields,
			Class<?>[] types) {
		mObjects = objects;
		mSymbols = symbols;
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

		set(mFieldLookup.get(field), o);
	}

	public void setSymbol(int i, String s) {
		mObjects[i] = null;
		mSymbols[i] = s;

		// invalidate serialized representation
		mBytes = null;
	}

	public void setSymbol(String field, String s) {
		if (mFieldLookup == null)
			initLookup();

		setSymbol(mFieldLookup.get(field), s);
	}

	public Object get(int i) {
		return mObjects[i];
	}

	public Object get(String field) {
		if (mFieldLookup == null)
			initLookup();

		return get(mFieldLookup.get(field));
	}

	public String getSymbol(int i) {
		if (mObjects[i] != null)
			return null;

		return mSymbols[i];
	}

	public String getSymbol(String field) {
		if (mFieldLookup == null)
			initLookup();

		return getSymbol(mFieldLookup.get(field));
	}

	public boolean containsSymbol(int i) {
		return mObjects[i] == null;
	}

	public boolean containsSymbol(String field) {
		if (mFieldLookup == null)
			initLookup();

		return containsSymbol(mFieldLookup.get(field));
	}

	public Class<?> getFieldType(int i) {
		return mTypes[i];
	}

	public Class<?> getFieldType(String field) {
		if (mFieldLookup == null)
			initLookup();

		return getFieldType(mFieldLookup.get(field));
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

	/**
	 * Returns a byte array representation of this tuple. This is used to
	 * determine the natural sort order of tuples, but useful for little else.
	 * 
	 * @return byte array representation of this tuple
	 */
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
				if (mObjects[i] == null) {
					out.writeUTF(mSymbols[i]);
				} else if (mTypes[i] == Integer.class) {
					out.writeInt((Integer) mObjects[i]);
				} else if (mTypes[i] == Boolean.class) {
					out.writeBoolean((Boolean) mObjects[i]);
				} else if (mTypes[i] == Long.class) {
					out.writeLong((Long) mObjects[i]);
				} else if (mTypes[i] == Float.class) {
					out.writeFloat((Float) mObjects[i]);
				} else if (mTypes[i] == Double.class) {
					out.writeDouble((Double) mObjects[i]);
				} else if (mTypes[i] == String.class) {
					out.writeUTF(mObjects[i].toString());
				} else {
					ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
					DataOutputStream dataOut = new DataOutputStream(bytesOut);

					((Writable) mObjects[i]).write(dataOut);
					out.write(bytesOut.toByteArray());
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
		mSymbols = new String[numFields];
		mFields = new String[numFields];
		mTypes = new Class[numFields];

		for (int i = 0; i < numFields; i++) {
			mFields[i] = in.readUTF();
		}

		for (int i = 0; i < numFields; i++) {
			byte type = in.readByte();

			if (type == SYMBOL) {
				String className = in.readUTF();
				try {
					mTypes[i] = Class.forName(className);
				} catch (Exception e) {
					e.printStackTrace();
				}
				mObjects[i] = null;
				mSymbols[i] = in.readUTF();
			} else if (type == INT) {
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
			} else if (type == STRING) {
				mTypes[i] = String.class;
				mObjects[i] = in.readUTF();
			} else {
				try {
					String className = in.readUTF();
					mTypes[i] = Class.forName(className);

					int sz = in.readInt();
					byte[] bytes = new byte[sz];
					in.readFully(bytes);

					Writable obj = (Writable) mTypes[i].newInstance();
					obj.readFields(new DataInputStream(
							new ByteArrayInputStream(bytes)));
					mObjects[i] = obj;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(mFields.length);
		for (int i = 0; i < mFields.length; i++) {
			out.writeUTF(mFields[i]);
		}

		for (int i = 0; i < mFields.length; i++) {
			if (mObjects[i] == null && mSymbols[i] == null) {
				throw new TupleException("Cannot serialize null fields!");
			}

			if (containsSymbol(i)) {
				out.writeByte(SYMBOL);
				out.writeUTF(mTypes[i].getCanonicalName());
				out.writeUTF(mSymbols[i]);
			} else if (mTypes[i] == Integer.class) {
				out.writeByte(INT);
				out.writeInt((Integer) mObjects[i]);
			} else if (mTypes[i] == Boolean.class) {
				out.writeByte(BOOLEAN);
				out.writeBoolean((Boolean) mObjects[i]);
			} else if (mTypes[i] == Long.class) {
				out.writeByte(LONG);
				out.writeLong((Long) mObjects[i]);
			} else if (mTypes[i] == Float.class) {
				out.writeByte(FLOAT);
				out.writeFloat((Float) mObjects[i]);
			} else if (mTypes[i] == Double.class) {
				out.writeByte(DOUBLE);
				out.writeDouble((Double) mObjects[i]);
			} else if (mTypes[i] == String.class) {
				out.writeByte(STRING);
				out.writeUTF(mObjects[i].toString());
			} else {
				out.writeByte(WRITABLE);

				ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
				DataOutputStream dataOut = new DataOutputStream(bytesOut);

				out.writeUTF(mTypes[i].getCanonicalName());
				((Writable) mObjects[i]).write(dataOut);
				out.writeInt(bytesOut.size());
				out.write(bytesOut.toByteArray());
			}
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < mFields.length; i++) {
			if (i != 0)
				sb.append(", ");
			if (mSymbols[i] != null) {
				sb.append(mSymbols[i]);
			} else {
				sb.append(mObjects[i]);
			}
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
