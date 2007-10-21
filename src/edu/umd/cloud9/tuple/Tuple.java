package edu.umd.cloud9.tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;

public class Tuple {

	protected static final int INT = -1;
	protected static final int LONG = -2;
	protected static final int FLOAT = -3;
	protected static final int DOUBLE = -4;

	private Object[] mObjects;
	private Schema mSchema;

	public Tuple(Object[] objs, Schema schema) {
		mObjects = objs;
		mSchema = schema;
	}

	public void Set(int i, Object o) {
		mObjects[i] = o;
	}

	public Object get(int i) {
		return mObjects[i];
	}

	public Object get(String field) {
		return mObjects[mSchema.getFieldIndex(field)];
	}

	public byte[] pack() {
		ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
		DataOutputStream dataOutStream = new DataOutputStream(byteOutStream);

		try {
			for (int i = 0; i < mObjects.length; i++) {
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
			while (dataInStream.available() != 0) {
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

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return schema.instantiate(objs.toArray());
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < mObjects.length; i++) {
			if (i != 0)
				sb.append(", ");
			sb.append(mObjects[i]);
		}

		return "(" + sb.toString() + ")";
	}
}
