package edu.umd.hooka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

import edu.umd.hooka.alignment.aer.ReferenceAlignment;

/**
 * 
 * @author chris
 * 
 *  Notes:
 *	This class represents a pair of phrases, one from target language, one from source language, and an alignment
 *
 */
public class PhrasePair implements WritableComparable, Cloneable {

	private Phrase f;
	private Phrase e;
	private Alignment a;
	private AlignmentPosteriorGrid g;
	
	public Object clone() {
		Phrase nf = (Phrase)f.clone();
		Phrase ne = (Phrase)e.clone();
		Alignment na = (Alignment)a.clone();
		return new PhrasePair(nf, ne, na);
	}
	public int compareTo(Object o) {
		PhrasePair that = (PhrasePair)o;
		int c = that.f.compareTo(this.f);
		if (c != 0) { return c; }
		c = that.e.compareTo(this.e);
		return c;
	}
	
	public int hashCode() {
		return f.hashCode() * 31 + f.size();
	}
	
	public PhrasePair getTranspose() {
		PhrasePair res = new PhrasePair(e, f, a);
		return res;
	}
	
	public PhrasePair() {
		e = new Phrase();
		f = new Phrase();
		a = null;
	}
	
	public PhrasePair(Phrase f, Phrase e) {
		this.f = f;
		this.e = e;
		this.a = null;
	}

	public PhrasePair(Phrase f, Phrase e, Alignment a) {
		this.f = f;
		this.e = e;
		this.a = a;
	}
	
	public Alignment getAlignment() {
		return a;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof PhrasePair)) {
			return false;
		}
		PhrasePair that = (PhrasePair)o;
		if (this.a != null) {
			if (that == null ||	!that.a.equals(this.a))
				return false;
		} else {
			if (that.a != null) return false;
		}
		return (e.equals(that.e) && f.equals(that.f));
	}
	
	public PhrasePair(String f, Vocab vocF, String e, Vocab vocE, String a)
	{
		this.f = Phrase.fromString(1, f, vocF);
		this.e = Phrase.fromString(0, e, vocE);
		if (a != null || !a.equals("")) {
			this.a = new Alignment(this.f.size(), this.e.size(), a);
		}
	}
	
	public float ratioFtoE() {
		return ((float)this.f.size()) / ((float)this.e.size());
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{F:").append(f).append(" ||| E:").append(e);
		if (a != null) { sb.append(" ||| A: ").append(a); }
		sb.append("}");
		return sb.toString();
	}
	public void mergeEnglishWords(int i, int j, int newE) {
		int elen = e.size();
		if (j >= elen)
			throw new IllegalArgumentException("mergeEnglishWords argument out of range j=" + j);
		if (i >= elen)
			throw new IllegalArgumentException("mergeEnglishWords argument out of range i=" + i);
		if (i == j)
			throw new IllegalArgumentException("i cannot equal j");
		int[] nep = new int[elen - 1];
		int[] ep = e.getWords();
		int d=0;
		for (int k = 0; k < elen-1; k++) {
			if ((k+d) == i) {
				nep[k] = newE;
				continue;
			}
			if (k == j)
				d++;
			nep[k] = ep[k+d];
		}
		e = new Phrase(nep,e.getLanguage());
		if (a != null)
			a = a.mergeEnglishWords(i, j);
	}
	public void splitEnglishWords(int i, int newE1, int newE2) {
		int elen = e.size();
		if (i >= elen)
			throw new IllegalArgumentException("splitEnglishWords argument out of range: " + i);
		int[] nep = new int[elen + 1];
		int[] ep = e.getWords();
		for (int k = 0; k < elen; k++) {
			if (k == i) {
				nep[k]   = newE1;
				nep[k+1] = newE2;
			} else if (k < i)
				nep[k] = ep[k];
			else if (k > i)
				nep[k+1] = ep[k];
		}
		e = new Phrase(nep,e.getLanguage());
		if (a != null)
			a = a.splitEnglishWords(i);
	}
	public void splitForeignWords(int j, int newF1, int newF2) {
		int flen = f.size();
		if (j >= flen)
			throw new IllegalArgumentException("splitForeignWords argument out of range: " + j);
		int[] nfp = new int[flen + 1];
		int[] fp = f.getWords();
		for (int k = 0; k < flen; k++) {
			if (k == j) {
				nfp[k]   = newF1;
				nfp[k+1] = newF2;
			} else if (k < j)
				nfp[k] = fp[k];
			else if (k > j)
				nfp[k+1] = fp[k];
		}
		f = new Phrase(nfp,f.getLanguage());
		if (a != null)
			a = a.splitForeignWords(j);
	}
	public String toString(Vocab vocF, Vocab vocE) {
		StringBuffer sb = new StringBuffer();
		sb.append(f.toString(vocF)).append(" ||| ").append(e.toString(vocE));
		if (hasAlignment()) {
			sb.append(" ||| ").append(a.toString());
		}
		return sb.toString();
	}
	
	public Phrase getE() {
		return e;
	}
	
	public Phrase getF() {
		return f;
	}
	
	public void setE(Phrase e) {
		this.e = e;
	}
	
	public void setF(Phrase f) {
		this.f = f;
	}

	public boolean hasAlignment() {
		return a != null;
	}
	
	public void setAlignment(Alignment a) {
		if (a == null) { this.a = null; return; }
		if (a.getELength() != e.size() ||
				a.getFLength() != f.size())
			throw new IllegalArgumentException("Mismatch p.e="+ e.size() + "a.e=" + a.getELength() + "  p.f=" + f.size() + " a.f=" + a.getFLength() );
		this.a = a;
	}
	
	public void readFields(DataInput in) throws IOException {
		f.readFields(in);
		e.readFields(in);
		byte at = in.readByte();
		a=null;
		if (at != 0) {
			//System.out.println("Reading " +f.size() + "--" + e.size());
			if (at == 1) 
				a = new Alignment(f.size(), e.size());
			else if (at == 2)
				a = new ReferenceAlignment(f.size(), e.size());
			else
				throw new IOException("bad format! at="+at);
			a.readFields(in);
			assert(a.getELength() == e.getWords().length);
			assert(a.getFLength() == f.getWords().length);
		}
		boolean hasg = in.readBoolean();
		if (hasg) {
			g = new AlignmentPosteriorGrid(this);
			g.readFiles(in);
		}
	}

	public void write(DataOutput out) throws IOException {
		f.write(out);
		e.write(out);
		if (hasAlignment()) {
			out.writeByte(a.getType());
			a.write(out);
		} else {
			out.writeByte(0);
		}
		if (hasAlignmentPosteriors()) {
			out.writeBoolean(true);
			g.write(out);
		} else {
			out.writeBoolean(false);
		}
	}
	
	public boolean hasAlignmentPosteriors() {
		return (g != null);
	}
	
	public AlignmentPosteriorGrid getAlignmentPosteriorGrid() {
		return g;
	}
	
	public void setAlignmentPosteriorGrid(AlignmentPosteriorGrid g) {
		this.g = g;
	}
		
	public static final class SubPhraseCoordinates {
		public int e_start;
		public int e_end;
		public int f_start;
		public int f_end;
		public SubPhraseCoordinates() {}
		public SubPhraseCoordinates(int es,int ee, int fs, int fe) {
			e_start = es;
			e_end = ee;
			f_start = fs;
			f_end = fe;
		}
		
		public String toString() {
			return "<(" + f_start + "," + f_end + ")-(" + e_start + "," + e_end +")>";
		}
	}

	public PhrasePair extractSubPhrasePair(SubPhraseCoordinates c) {
		return extractSubPhrasePair(c.f_start, c.f_end, c.e_start, c.e_end);
	}

	public PhrasePair extractSubPhrasePair(int startF, int endF, int startE, int endE)
	{
		PhrasePair n = new PhrasePair();
		n.e = this.e.getSubPhrase(startE,endE);
		n.f = this.f.getSubPhrase(startF,endF);
		n.a = new Alignment(endF - startF + 1, endE - startE + 1);
		for (int fi = startF; fi <= endF; fi++)
			for (int ei = startE; ei <= endE; ei++)
				if (this.a.aligned(fi, ei))
					n.a.align(fi - startF, ei - startE);
		return n;
	}

	/**
	 * Returns the smallest consistent phrase pair that contains [e_start,e_end]
	 * This is not efficient- don't use it where speed counts!
	 */
	public SubPhraseCoordinates getMinimalConsistentSubPhraseCoordsContainingESpan(int e_start, int e_end) {
		int elen = e.size();
		int flen = f.size();
		int ne_s = e_start;
		int ne_e = e_end;
		while (ne_s > 0    && !a.isEAligned(ne_s)) { ne_s--; }  // is start aligned? if not, keep moving left
		while (ne_e < elen && !a.isEAligned(ne_e)) { ne_e++; }  // is end aligned? if not, keep moving right
		if (ne_s <  0   ) { ne_s = 0;       } // make sure left edge isn't less than 0
		if (ne_e >= elen) { ne_e = elen-1;  } // make sure right edge isn't > len
		// at this point, e_start and e_end are aligned to f words, so find the f range
		boolean isConsistent = false;
		int maxF = -1;
		int minF = 9999999;
		while(!isConsistent) {
			isConsistent = true;
			maxF = -1;
			minF = 9999999;
			//System.err.println("ne_s:" + ne_s +"\tne_e:" + ne_e + "\telen:"+elen);
			for (int e = ne_s; e <= ne_e; e++) {		
				for (int f = 0; f<flen; f++) {
					if (a.aligned(f, e)) {
						if (f > maxF) maxF = f;
						if (f < minF) minF = f;
					}
				}
				if (maxF == -1) { maxF = flen - 1; }
				if (minF == 9999999) { minF = 0; }
			}
			for (int f = minF; f <= maxF; f++) {		
				for (int e = 0; e<elen; e++) {
					if (a.aligned(f, e)) {
						if (e > ne_e) {ne_e = e; isConsistent = false; }
						if (e < ne_s) {ne_s = e; isConsistent = false; }
					}
				}
			}
		}
		return new SubPhraseCoordinates(ne_s, ne_e, minF, maxF);
	}

	public PhrasePair extractMinimalConsistentPhrasePairContainingESpan(int eStart, int eEnd) {
		SubPhraseCoordinates spc = getMinimalConsistentSubPhraseCoordsContainingESpan(eStart,eEnd);
		return extractSubPhrasePair(spc.f_start, spc.f_end, spc.e_start, spc.e_end);
	}
			
	public ArrayList<SubPhraseCoordinates> extractConsistentSubPhraseCoordinates(int maxPhraseLength)
	{
		ArrayList<SubPhraseCoordinates> res = new ArrayList<SubPhraseCoordinates>();
		int _elen = e.size();
		int _flen = f.size();
		if (!this.hasAlignment())
			throw new RuntimeException("Missing alignment");
		
		int[] alignedCountF = new int[_flen];
		ArrayList<ArrayList<Integer> > alignedToE = new ArrayList<ArrayList<Integer> >();
		for (int i=0; i<_elen; i++)
		{
			alignedToE.add(new ArrayList<Integer>());
		}
		java.util.Iterator<Alignment.IntPair> ai = a.iterator();
		while (ai.hasNext())
		{
			Alignment.IntPair pair = ai.next();
			int f = pair.f;
			int e = pair.e;
			alignedToE.get(e).add(f);
			alignedCountF[f]++;
		}
		int[] usedF = new int[alignedCountF.length];
		//for (int cc=0; cc<_flen; cc++) {
		//	System.out.println(" " + cc + ": " + alignedCountF[cc]);
		//}
		
		for (int startE=0; startE<_elen; startE++) {
			for (int endE=startE; (endE<_elen && endE<startE+maxPhraseLength); endE++)
			{
				int maxF = -1;
				int minF = 9999999;
				System.arraycopy(alignedCountF, 0, usedF, 0, usedF.length);
				for (int ei=startE; ei<=endE; ei++) {
					ArrayList<Integer> alignedToEi = alignedToE.get(ei);
					int naei = alignedToEi.size();
					for (int i=0; i<naei; i++) {
						int fi = alignedToEi.get(i).intValue();
						if (fi < minF) { minF = fi; }
						if (fi > maxF) { maxF = fi; }
						usedF[fi]--;
					}
				}
				
				if (maxF >= 0 &&
						maxF - minF < maxPhraseLength)
				{
					boolean oob = false;
					for (int fi=minF;fi<=maxF && !oob;fi++) {
						if (usedF[fi] > 0) { oob = true; }
					}
					if (!oob) {
						for (int startF = minF;
							       (startF>=0 &&
									startF>maxF - maxPhraseLength &&
									(startF==minF || alignedCountF[startF]==0)); startF--) {
							for (int endF = maxF;
									(endF < _flen && endF < startF + maxPhraseLength &&
									 (endF == maxF || alignedCountF[endF] == 0)); endF++) {
								res.add(new SubPhraseCoordinates(startE,endE, startF, endF));
							}						
						}
					}
				}
			}
		}
		return res;
	}
	
	public ArrayList<PhrasePair> extractConsistentPhrasePairs(int maxPhraseLength)
	{
		ArrayList<SubPhraseCoordinates> pcl = extractConsistentSubPhraseCoordinates(maxPhraseLength);
		ArrayList<PhrasePair> res = new ArrayList<PhrasePair>(pcl.size());
		for (SubPhraseCoordinates spc : pcl) 
			res.add(this.extractSubPhrasePair(spc));
		return res;
	}
}
