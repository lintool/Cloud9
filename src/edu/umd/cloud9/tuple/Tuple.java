package edu.umd.cloud9.tuple;

public class Tuple {

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
		return mObjects[mSchema.getColumnIndex(field)];
	}

	public String pack() {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < mObjects.length; i++) {
			if (i != 0)
				sb.append(", ");
			sb.append(mObjects[i]);
		}
		
		return "(" + sb.toString() + ")";
	}
}
