package edu.umd.hooka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import java.nio.*;

/**
 * Represents a string of one or more words that may be a word,
 * phrase, sentence, or unit larger than a sentence.
 * 
 * In this version, each word has an int attached to it.
 * 
 * @author redpony
 */
public class FloatAugmentedPhrase implements WritableComparable, Cloneable {

	byte _language;
	int[] _words;
	float[] _values;
	
	public Object clone() {
		FloatAugmentedPhrase res = new FloatAugmentedPhrase();
		res._language = _language;
		if (_words != null) {
			res._words = _words.clone();
		}
		if (_values != null) {
			res._values = _values.clone();
		}
		return res;
	}
	
	public FloatAugmentedPhrase() {}
	public FloatAugmentedPhrase(int[] p, int lang, float[] v) {
		_language = (byte)lang;
		_words = p;
		_values = v;
	}
	
	public int size() {
		if (_words==null) return 0; else return _words.length;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof FloatAugmentedPhrase))
			return false;
		FloatAugmentedPhrase that=(FloatAugmentedPhrase)o;
		if (this._language != that._language)
			return false;
		if (that._words.length != this._words.length)
			return false;
		if (java.util.Arrays.equals(this._words, that._words))
			return false;
		else return java.util.Arrays.equals(this._values, that._values);
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[L=").append(_language);
		if (_words != null) {
			for (int i=0; i<_words.length; ++i) {
				sb.append(' ').append(_words[i]);
			}
		}
		if (_values != null) {
			for (int i=0; i<_words.length; ++i) {
				sb.append(' ').append(_values[i]);
			}
		}
		sb.append(']');
		return sb.toString();
	}
	
	public int compareTo(Object o)
	{
		FloatAugmentedPhrase that = (FloatAugmentedPhrase)o;
		if (_language != that._language) {
			return (int)that._language - (int)this._language; 
		}
		if (this._words == null || that._words == null) {
			int a = 0; if (_words!=null) a = _words.length;
			int b = 0; if (that._words!=null) b = that._words.length;
			return b - a;
		}
		for (int i=0; i < _words.length && i < that._words.length; i++) {
			int a = _words[i];
			int b = that._words[i];
			if (a != b) return a - b;
		}
		return _words.length - that._words.length;
	}
	
	public int hashCode() {
		int hc = (int)_language + 73;
		if (_words != null) {
			for (int i = 0; i < _words.length; i++) {
				hc = (31 * hc) + _words[i];
			}
		}
		if (_values != null) {
			for (int i = 0; i < _values.length; i++) {
				hc = (31 * hc) + (int)(_values[i] * 31415.9);
			}
		}
		return hc;
	}
	
	public String toString(Vocab voc) {
		StringBuffer sb = new StringBuffer();
		if (_words != null) {
			for (int i=0; i<_words.length; ++i) {
				if (i != 0) sb.append(' ');
				sb.append(voc.get(_words[i]));
			}
		}
		if (_values != null) {
			for (int i=0; i<_values.length; ++i) {
				if (i != 0) sb.append(' ');
				sb.append(_values[i]);
			}
		}
		return sb.toString();		
	}
	
	public int[] getWords() { return _words; }
	public float[] getValues() { return _values; }
	
	public byte getLanguage() { return _language; }
	public void setLanguage(int l) { _language = (byte)l; }
	
	public FloatAugmentedPhrase getSubPhrase(int start, int end)
	{
		FloatAugmentedPhrase res = new FloatAugmentedPhrase();
		res._language = _language;
		res._words = new int[end-start+1];
		System.arraycopy(_words, start, res._words, 0, end-start+1);
		System.arraycopy(_values, start, res._values, 0, end-start+1);
		return res;
	}
	
	public static FloatAugmentedPhrase fromString(int lang, String sentence, Vocab voc)
	{
		throw new UnsupportedOperationException();
		/*
		FloatAugmentedPhrase s = new FloatAugmentedPhrase();
		s._language = (byte)lang;
		String[] w=sentence.split("\\s+");
		s._words = new int[w.length];
		for (int i=0; i<w.length; i++) {
			s._words[i] = voc.addOrGet(w[i]);
		}
		return s;*/
	}

	public void readFields(DataInput in) throws IOException {
		_language = in.readByte();
		int bbLen = in.readInt();
		//read words
		if (bbLen == 0) { _words = null; return; }
		ByteBuffer bb=ByteBuffer.allocate(bbLen);
		in.readFully(bb.array());
		IntBuffer ib = bb.asIntBuffer();
		_words = new int[bbLen/4];
		ib.get(_words);
		//read values
		if (bbLen == 0) { _values = null; return; }
		bb=ByteBuffer.allocate(bbLen);
		in.readFully(bb.array());
		FloatBuffer fb = bb.asFloatBuffer();
		_values = new float[bbLen/4];
		fb.get(_values);
	}
	
	public void setWords(int[] words) {
		this._words = words;
	}
	
	public void setValues(float[] values) {
		this._values = values;
	}

	public void write(DataOutput out) throws IOException {
		out.writeByte(_language);
		int bbLen = 0;
		if (_words != null) { bbLen = _words.length * 4; }
		out.writeInt(bbLen);
		if (bbLen == 0) { return; }
		ByteBuffer bb=ByteBuffer.allocate(bbLen);
		IntBuffer ib = bb.asIntBuffer();
		ib.put(_words);
		out.write(bb.array());
		bb=ByteBuffer.allocate(bbLen);
		FloatBuffer fb = bb.asFloatBuffer();
		fb.put(_values);
		out.write(bb.array());
	}
	
}
