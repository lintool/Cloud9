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

	private DataOutputStream mDataRep;
	private ByteArrayOutputStream mByteRep;
	
	private Map<String, Integer> mFieldLookup = null;

	protected Tuple(Object[] objs, String[] fields, Class[] types) {
		mObjects = objs;
		mFields = fields;
		mTypes = types;
	}

	// this is bad idea, but that's how WritableComparables are created
	public Tuple() {
	}

	public static Tuple createFrom(DataInput in) throws IOException {
		Tuple tuple = new Tuple();
		tuple.readFields(in);

		return tuple;
	}

	public void set(int i, Object o) {
		mObjects[i] = o;
	}

	public Object get(int i) {
		return mObjects[i];
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
	
	public Object get(String field) {
		if ( mFieldLookup == null)
			initLookup();
		
		return mObjects[mFieldLookup.get(field)];
	}

	public byte[] getBytes() {
		try {
		mByteRep = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(mByteRep);
		
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
		
		return mByteRep.toByteArray();
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

	/*
	public byte[] pack() {
		ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
		DataOutputStream dataOutStream = new DataOutputStream(byteOutStream);

		try {
			for (int i = 0; i < mSchema.getFieldCount(); i++) {
				if (mSchema.getFieldType(i) == Integer.class) {
					dataOutStream.writeInt(INT);
					dataOutStream.writeInt((Integer) mObjects[i]);
				} else if (mSchema.getFieldType(i) == Long.class) {
					dataOutStream.writeInt(LONG);
					dataOutStream.writeLong((Long) mObjects[i]);
				} else if (mSchema.getFieldType(i) == Float.class) {
					dataOutStream.writeInt(FLOAT);
					dataOutStream.writeFloat((Float) mObjects[i]);
				} else if (mSchema.getFieldType(i) == Double.class) {
					dataOutStream.writeInt(DOUBLE);
					dataOutStream.writeDouble((Double) mObjects[i]);
				} else {
					byte[] data = mObjects[i].toString().getBytes("utf8");
					dataOutStream.writeInt(data.length);
					dataOutStream.write(data);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return byteOutStream.toByteArray();
	}

	static public Tuple unpack(byte[] bytes, Schema schema) {
		DataInputStream dataInStream = new DataInputStream(
				new ByteArrayInputStream(bytes));

		ArrayList<Object> objs = new ArrayList<Object>();
		try {
			int i = 0;
			while (i < schema.getFieldCount()) {
				int type = dataInStream.readInt();

				if (type == INT) {
					objs.add(dataInStream.readInt());
				} else if (type == LONG) {
					objs.add(dataInStream.readLong());
				} else if (type == FLOAT) {
					objs.add(dataInStream.readFloat());
				} else if (type == DOUBLE) {
					objs.add(dataInStream.readDouble());
				} else {
					byte[] in = new byte[type];
					dataInStream.read(in);
					objs.add(new String(in, "utf8"));
				}
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return schema.instantiate(objs.toArray());
	}

	static public void unpackInto(Tuple tuple, byte[] bytes) {
		DataInputStream dataInStream = new DataInputStream(
				new ByteArrayInputStream(bytes));

		ArrayList<Object> objs = new ArrayList<Object>();
		try {
			int i = 0;
			while (i < tuple.getSchema().getFieldCount()) {
				int type = dataInStream.readInt();

				if (type == INT) {
					objs.add(dataInStream.readInt());
				} else if (type == LONG) {
					objs.add(dataInStream.readLong());
				} else if (type == FLOAT) {
					objs.add(dataInStream.readFloat());
				} else if (type == DOUBLE) {
					objs.add(dataInStream.readDouble());
				} else {
					byte[] in = new byte[type];
					dataInStream.read(in);
					objs.add(new String(in, "utf8"));
				}
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		for (int i = 0; i < objs.size(); i++) {
			tuple.set(i, objs.get(i));
		}

	}
*/
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
		byte[] thatObj = ((Tuple) obj).getBytes();
		byte[] thisObj = this.getBytes();
		
		return WritableComparator.compareBytes(thisObj, 0, thisObj.length, thatObj, 0, thatObj.length);
	}
	
	  public int hashCode() {
		  byte[] bytes = getBytes();
		    return WritableComparator.hashBytes(bytes, bytes.length);
		  //return toString().hashCode();
		  }

	  /*
	public int compareTo(Object obj) {
		
		return this.toString().compareTo(obj.toString());
	}*/
	
	
	
	  /** A Comparator optimized for Tuple. */ 
	  public static class Comparator extends WritableComparator {
	    public Comparator() {
	      super(Tuple.class);
	    }
	    
	    /**
	     * Compare the buffers in serialized form.
	     */
	    public int compare(byte[] b1, int s1, int l1,
	                       byte[] b2, int s2, int l2) {
	      return compareBytes(b1, s1, l1, b2, s2, l2);
	    }
	  }
	  
	  static {                                        // register this comparator
	    WritableComparator.define(Tuple.class, new Comparator());
	  }
	  
}
