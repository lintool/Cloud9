package edu.umd.hooka.alignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.hooka.alignment.hmm.ATable;

/**
 * Store anything that stores partial counts.
 * 
 * @author redpony
 *
 */
public class PartialCountContainer implements Writable, Cloneable {

	public static final int CONTENT_ATABLE = 2;
	public static final int CONTENT_ARRAY  = 3;
	
	byte type = 0;
	Writable content = null;
	
	public PartialCountContainer() {}
	
	private PartialCountContainer(Writable c, byte t) {
		content = c;
		type = t;
	}

	public PartialCountContainer(Writable content) {
		this.setContent(content);
	}
	
	public Object clone() {
		Writable nc = null;
		if (type == CONTENT_ATABLE)
			nc = (Writable)((ATable)content).clone();
		else if (type == CONTENT_ARRAY)
			nc = (Writable)((IndexedFloatArray)content).clone();
		else throw new RuntimeException("Bad type");
		return new PartialCountContainer(nc, type);
	}
	
	public void setContent(Writable content) {
		if (content instanceof ATable)
			type=CONTENT_ATABLE;
		else if (content instanceof IndexedFloatArray)
			type=CONTENT_ARRAY;
		else throw new RuntimeException("Don't know how to wrap " + content);
		this.content = content;		
	}
	
	public Writable getContent() {
		return content;
	}
	
	public int getType() {
		return type;
	}
	
	public void plusEquals(PartialCountContainer rhs) {
		if (rhs.type != this.type)
			throw new RuntimeException("Type mismatch!");
		else if (type == CONTENT_ATABLE)
			((ATable)content).plusEquals((ATable)rhs.content);
		else if (type == CONTENT_ARRAY)
			((IndexedFloatArray)content).plusEquals((IndexedFloatArray)rhs.content);
		else throw new RuntimeException("Bad type");
	}

	/**
	 * TODO: atable normalization currently doesn't support
	 * VB or an alpha parameter.  This should probably be a
	 * separate param, ie. alpha2 and vb 2.
	 * 
	 * @param variationalBayes
	 * @param alpha
	 */
	public void normalize(boolean variationalBayes, float alpha) {
		if (type == CONTENT_ATABLE)
			((ATable)content).normalize();
		else if (type == CONTENT_ARRAY) {
			if (variationalBayes)
				((IndexedFloatArray)content).normalize_variationalBayes(alpha);
			else
				((IndexedFloatArray)content).normalize(alpha);
		} else throw new RuntimeException("Bad type");
	}

	public void readFields(DataInput in) throws IOException {
		type = in.readByte();
		if (type == CONTENT_ATABLE) {
			content = new ATable();
		} else if (type == CONTENT_ARRAY) {
			content = new IndexedFloatArray();
		}else {
			throw new RuntimeException("Bad content type!");
		}
		content.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeByte(type);
		content.write(out);
	}
	
	public String toString() {
		return "T(" + type + "): " + content;
	}

}
