package edu.umd.hooka.alignment.aer;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

import edu.umd.hooka.Alignment;

public class ReferenceAlignment extends Alignment {

	M2 _sureAligned;
	
	public ReferenceAlignment() { _sureAligned = new M2(); }
	public ReferenceAlignment(int flen, int elen) {
		super(flen, elen);
		_sureAligned = new M2(flen, elen);
	}
	
	@Override
	public byte getType() {
		return 2;
	}
	
	public void sureAlign(int f, int e) {
		super.align(f, e);
		_sureAligned.set(f,e);
	}
	
	public void probableAlign(int f, int e) {
		super.align(f, e);
	}
	
	public boolean isSureAligned(int f, int e) {
		return _sureAligned.get(f, e);
	}
	
	public boolean isProbableAligned(int f, int e) {
		return super.aligned(f, e);
	}
	
	public int countProbableHits(Alignment test) {
//		if (_flen > test.getFLength() || _elen > test.getELength())
//			throw new RuntimeException("Reference is larger than source! " + _elen + "," + _flen + "  " + test.getELength() + "," + test.getFLength());
		int hits = 0;
		int fl = Math.min(_flen, test.getFLength());
		int el = Math.min(_elen, test.getELength());
		for (int j=0; j < fl; j++)
			for (int i = 0; i < el; i++)
				if (this.aligned(j, i) && test.aligned(j, i))
					hits += 1;
		return hits;
	}

	public int countSureHits(Alignment test) {
		if (_flen != test.getFLength() || _elen != test.getELength())
			throw new RuntimeException("Reference is larger than source! " + _elen + "," + _flen + "  " + test.getELength() + "," + test.getFLength());
		int hits = 0;
		int fl = Math.min(_flen, test.getFLength());
		int el = Math.min(_elen, test.getELength());
		for (int j=0; j < fl; j++)
			for (int i = 0; i < el; i++)
				if (test.aligned(j, i) && this.isSureAligned(j, i))
					hits += 1;
		return hits;
	}

	public int countSureAlignmentPoints() {
		int count = 0;
		for (int j=0; j < _flen; j++)
			for (int i = 0; i < _elen; i++)
				if (this.isSureAligned(j, i))
					count += 1;
		return count;
	}
	
	@Override
	public Alignment getTranspose() {
		ReferenceAlignment ra = new ReferenceAlignment(_elen, _flen);
		for (int ei=0; ei<_elen; ei++)
			for (int fi=0; fi<_flen; fi++) {
				if (isProbableAligned(fi, ei))
					ra.probableAlign(ei, fi);
				if (isSureAligned(fi, ei))
					ra.sureAlign(ei, fi);
			}
		return ra;
	}
	
	public void addAlignmentPointsPharaoh(String p) {
		String[] aps = p.split("\\s+");
		for (String ap : aps) {
			if (ap.length() == 0) continue;
			boolean probable = (ap.charAt(0) == '?');
			if (probable) { ap = ap.substring(1); }
			String[] ef = ap.split("-");
			if (ef.length != 2)
				throw new RuntimeException("Invalid format: " + ap);
			int f = Integer.parseInt(ef[0]);
			int e = Integer.parseInt(ef[1]);
			if (probable)
				this.probableAlign(f, e);
			else
				this.sureAlign(f, e);
		}
	}
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		for (int i=0; i<_flen; i++)
			for (int j=0; j<_elen; j++)
				if (isSureAligned(i, j))
					sb.append(i).append('-').append(j).append(' ');
				else if (isProbableAligned(i, j))
					sb.append('?').append(i).append('-').append(j).append(' ');
		if (sb.length() > 0)
			sb.delete(sb.length()-1, sb.length());
		return sb.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		_sureAligned.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		_sureAligned.write(out);
	}
	
	static class EFSTriple {
		public EFSTriple(int e, int f, boolean sure) { this.e = e; this.f = f; this.sure = sure; }
		int e; int f; boolean sure;
	}

	static class WPTFileReader implements Iterator<ReferenceAlignment> {

		int maxE=-1;
		int maxF=-1;
		BufferedReader br = null;
		int cur = -1;
		String line = null;
		ArrayList<EFSTriple> aps = null;
		int blanks = 0;
		
		public WPTFileReader(InputStream in) throws IOException {
			br = new BufferedReader(new InputStreamReader(in));
			line = br.readLine();
			aps = new ArrayList<EFSTriple>();
		}
		
		public boolean hasNext() {
			return line != null;
		}

		public ReferenceAlignment next() {
			EFSTriple t = null;
			int ln = 0, e = 0, f = 0;
			if (blanks > 0) {
				blanks--;
				ReferenceAlignment r = new ReferenceAlignment();
				return r;
			}
			if (line == null)
				throw new IllegalStateException("No more lines!");
			try {
				do {
					String[] fs = line.split("\\s+");
					if (fs.length != 4)
						throw new RuntimeException("Expected format: LN E F P/S line="+line);
					ln = Integer.parseInt(fs[0]);
					e = Integer.parseInt(fs[1]);
					f = Integer.parseInt(fs[2]);
					boolean subtract = true;
					if (subtract) {
						e--; f--; }
					boolean sure = fs[3].equals("S");
					t = new EFSTriple(e, f, sure);
					if (cur == -1) { cur = ln; }
					if (ln != cur)
						break;
					if (e > maxE) maxE = e;
					if (f > maxF) maxF = f;
					aps.add(t);
				} while ((line = br.readLine()) != null);
			} catch (IOException ex) {
				throw new RuntimeException("Caught " + ex);
			}
			ReferenceAlignment r = new ReferenceAlignment(maxF+1, maxE+1);
			for (EFSTriple tt : aps) {
				if (tt.sure)
					r.sureAlign(tt.f, tt.e);
				else
					r.align(tt.f, tt.e);
			}
			maxE = -1;
			maxF = -1;
			blanks = ln - cur - 1;
			cur = ln;
			aps = new ArrayList<EFSTriple>();
			return r;
		}

		public void remove() {
			throw new IllegalStateException();
		}		
	}

	public static WPTFileReader getWPT03FileIterator(InputStream is) throws IOException {
		return new WPTFileReader(is);
	}
}
