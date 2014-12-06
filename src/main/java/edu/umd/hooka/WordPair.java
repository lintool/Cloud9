package edu.umd.hooka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public final class WordPair implements WritableComparable {

	int f_langWord;
	int e_langWord;
	
	public WordPair() {}
	
	public WordPair(int e, int f) {
		setE(e);
		setF(f);
	}
	
	public void setFLanguageCode(int lc) {
		lc <<= 24;
		f_langWord &= 0x00ffffff;
		f_langWord |= lc;
	}

	public void setELanguageCode(int lc) {
		lc <<= 24;
		e_langWord &= 0x00ffffff;
		e_langWord |= lc;
	}
	
	public byte getFLanguageCode() {
		return (byte)(f_langWord >> 24);
	}

	public byte getELanguageCode() {
		return (byte)(e_langWord >> 24);
	}
	
	public void setF(int f) {
		f_langWord &= 0xff000000;
		f_langWord |= f;
	}

	public void setE(int e) {
		e_langWord &= 0xff000000;
		e_langWord |= e;
	}
	
	public int getF() {
		return (f_langWord & 0x00ffffff);
	}

	public int getE() {
		return (e_langWord & 0x00ffffff);
	}
	
	public void makeEMarginal() {
		e_langWord = -1;
	}

	public boolean isEMarginal() {
		return (e_langWord == -1);
	}

	public void readFields(DataInput in) throws IOException {
		f_langWord = in.readInt();
		e_langWord = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(f_langWord);
		out.writeInt(e_langWord);
	}

	public int compareTo(Object o) {
		WordPair that = (WordPair)o;
		if (this.e_langWord != that.e_langWord)
			return this.e_langWord - that.e_langWord;
		return this.f_langWord - that.f_langWord;
	}
	
	public int hashCode() {
		return f_langWord;
	}
	
	public void set(WordPair rhs) {
		f_langWord = rhs.f_langWord;
		e_langWord = rhs.e_langWord;
	}
	
	public void swap() {
		int t = f_langWord;
		f_langWord = e_langWord;
		e_langWord = t;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{ F(").append(getFLanguageCode()).append("):")
		  .append(getF()).append(" , ");
		if (e_langWord == -1) {
			sb.append("* }");
		} else {
			sb.append("E(").append(getELanguageCode()).append("):")
			  .append(getE()).append(" }");
		}
		return sb.toString();
	}
	
	public String toString(Vocab vf, Vocab ve) {
		StringBuffer sb = new StringBuffer();
		sb.append("{ ").append(vf.get(getF())).append(" , ");
		if (e_langWord == -1) {
			sb.append("* }");
		} else {
			sb.append(ve.get(getE())).append(" }");
		}
		return sb.toString();
	}

}
