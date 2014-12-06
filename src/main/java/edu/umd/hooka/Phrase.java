package edu.umd.hooka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import java.nio.*;
import java.util.TreeSet;

/**
 * Represents a string of one or more words that may be a word,
 * phrase, sentence, or unit larger than a sentence.
 * 
 * @author chris
 */
public class Phrase implements WritableComparable, Cloneable {

	byte _language;
	int[] _words;
	
	public Object clone() {
		Phrase res = new Phrase();
		res._language = _language;
		if (_words != null) {
			res._words = _words.clone();
		}
		return res;
	}
	
	public Phrase() {}
	public Phrase(int[] p, int lang) {
		_language = (byte)lang;
		_words = p;
	}
	
	public int size() {
		if (_words==null) return 0; else return _words.length;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof Phrase))
			return false;
		Phrase that=(Phrase)o;
		if (this._language != that._language)
			return false;
		if (that._words.length != this._words.length)
			return false;
		return java.util.Arrays.equals(this._words, that._words);
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[L=").append(_language);
		if (_words != null) {
			for (int i=0; i<_words.length; ++i) {
				sb.append(' ').append(_words[i]);
			}
		}
		sb.append(']');
		return sb.toString();
	}
	
	public int compareTo(Object o)
	{
		Phrase that = (Phrase)o;
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
		return sb.toString();		
	}
	
	public int[] getWords() { return _words; }
	public TreeSet<Integer> getWordsWithoutDuplicates()
	{
    	TreeSet<Integer> vals = new TreeSet<Integer>();
		for(int i=0; i<_words.length; i++ ) {
			vals.add(new Integer(_words[i]));
	    }
		return vals;
	}
	
	public byte getLanguage() { return _language; }
	public void setLanguage(int l) { _language = (byte)l; }
	
	public Phrase getSubPhrase(int start, int end)
	{
		Phrase res = new Phrase();
		res._language = _language;
		res._words = new int[end-start+1];
		System.arraycopy(_words, start, res._words, 0, end-start+1);
		return res;
	}
	
	public static Phrase fromString(int lang, String sentence, Vocab voc)
	{
		Phrase s = new Phrase();
		s._language = (byte)lang;
		String[] w=sentence.split("\\s+");
		s._words = new int[w.length];
		for (int i=0; i<w.length; i++) {
			s._words[i] = voc.addOrGet(w[i]);
		}
		return s;
	}

	public void readFields(DataInput in) throws IOException {
		_language = in.readByte();
		int bbLen = in.readInt();
		if (bbLen == 0) { _words = null; return; }
		ByteBuffer bb=ByteBuffer.allocate(bbLen);
		in.readFully(bb.array());
		IntBuffer ib = bb.asIntBuffer();
		_words = new int[bbLen/4];
		ib.get(_words);
	}
	
	public void setWords(int[] words) {
		this._words = words;
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
	}
	
}
