package edu.umd.hooka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class Phrase2CountMap extends TreeMap<Phrase,FloatWritable>
  implements Writable {

	private static final long serialVersionUID = 1093017050863486402L;

	public final void plusEquals(Phrase2CountMap rhs) {
		for (Map.Entry<Phrase, FloatWritable> ri : rhs.entrySet()) {
			FloatWritable cv = this.get(ri.getKey());
			if (cv == null) {
				cv = new FloatWritable(0);
				this.put(ri.getKey(), cv);
			}
			cv.set(cv.get() + ri.getValue().get());
		}
	}
	
	public final void setPhraseCount(Phrase key, float value) {
		this.put(key, new FloatWritable(value));
	}
	
	public final float getPhraseCount(Phrase key) {
		FloatWritable x = this.get(key);
		if (x == null) return 0.0f;
		return x.get();
	}
	
	public void normalize() {
		float total = 0.0f;
		for (Map.Entry<Phrase, FloatWritable> i : this.entrySet())
			total += i.getValue().get();
		if (total > 0.0f) 
			for (Map.Entry<Phrase, FloatWritable> i : this.entrySet()) {
				FloatWritable cur = i.getValue();
				cur.set(cur.get() / total);		
			}
		else
			throw new RuntimeException(this +"\ntotal=0.0 : please implement uniform distribution");
	}
	
	public void readFields(DataInput in) throws IOException {
		this.clear();
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			Phrase p = new Phrase();
			FloatWritable c = new FloatWritable();
			p.readFields(in);
			c.readFields(in);
			this.put(p, c);
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		for (Map.Entry<Phrase, FloatWritable> i : this.entrySet()) {
			i.getKey().write(out);
			i.getValue().write(out);
		}
	}

}
