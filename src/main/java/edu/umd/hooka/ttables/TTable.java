package edu.umd.hooka.ttables;

import java.io.IOException;

import edu.umd.hooka.PhrasePair;
import edu.umd.hooka.alignment.IndexedFloatArray;

public abstract class TTable implements Cloneable {
	public abstract Object clone();
	public abstract void add(int e, int f, float delta);
	public abstract void set(int e, int f, float value);
	public abstract void set(int e, IndexedFloatArray fs);
	public abstract float get(int e, int f);
	public abstract void clear();	
	public abstract void normalize();
	public abstract void write() throws IOException;

	public void prepare(PhrasePair pp, boolean nullWord) {}
}
