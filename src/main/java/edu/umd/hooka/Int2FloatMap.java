package edu.umd.hooka;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;

import edu.umd.hooka.alignment.IndexedFloatArray;

public final class Int2FloatMap {
	//TODO the performance of this class can be improved
	TreeMap<Integer, FloatWritable> data = new TreeMap<Integer, FloatWritable>();

	public Int2FloatMap() {}
		
	public final void increment(int k, float delta) {
		FloatWritable cvF = data.get(k);
		cvF.set(cvF.get() + delta);
	}
	
	public final Set<Map.Entry<Integer, FloatWritable>> entrySet() {
		return data.entrySet();
	}
	
	public final int maxKey() {
		return data.lastKey();
	}
	
	public final void createIfMissing(int k) {
		Integer ki = new Integer(k);
		if (data.get(ki) == null) {
			FloatWritable n = new FloatWritable();
			data.put(k, n);
		}
	}
	
	public final void set(int k, float value) {
		data.get(k).set(value);
	}
	
	public final float get(int k) {
		return data.get(k).get();
	}
	
	public final FloatWritable getFloatWritable(int k) {
		return data.get(k);
	}
	
	public IndexedFloatArray getAsIndexedFloatArray() {
		int[] indices = new int[data.size()];
		float[] values = new float[data.size()];
		int c = 0;
		for (Map.Entry<Integer, FloatWritable> p : data.entrySet()) {
			indices[c] = p.getKey();
			values[c] = p.getValue().get();
			c++;
		}
		return new IndexedFloatArray(indices, values);
	}
}