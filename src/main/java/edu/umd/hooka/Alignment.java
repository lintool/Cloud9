package edu.umd.hooka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Alignment implements org.apache.hadoop.io.Writable, 
  java.lang.Iterable<Alignment.IntPair>,
  Cloneable {
	public class IntPair {
		public int f;
		public int e;
		public IntPair(int f,int e) { this.f = f; this.e = e; }
		public String toString() {
			return f + "-" + e;
		}
	}
	public class AIterator implements java.util.Iterator<IntPair> {
		int cur;
		boolean[] d;
		int w;
	
		protected AIterator(Alignment a) {
			this.d = a._aligned._data;
			this.w = a._aligned._w;
			this.cur = 0;
			advance();
		}
		
		protected void advance() {
			while (cur < d.length && !d[cur]) { cur++; }
		}
		
		public boolean hasNext() {
			return cur < d.length;
		}
		
		public IntPair next() {
			IntPair res = new IntPair(cur % w, cur / w);
			cur++; advance();
			return res;
		}
		
		public void remove() {
			return;
		}
		
	}
	
	protected final static class M2 implements Cloneable{
		public short _w;
		public boolean[] _data;
		public Object clone() {
			M2 res = new M2();
			res._data = _data.clone();
			res._w = _w;
			return res;
		}
		public M2() { _w =0; _data = null; }
		public M2(int f, int e) {
			//System.err.println("x:"+x+"y:"+y);
			_data = new boolean[f*e];
			_w = (short)f;
		}
		void eraseFirstEWord() {
			boolean[] nd = new boolean[_data.length - _w];
			System.arraycopy(nd, 0, _data, 0, _data.length-_w);
			_data = nd;
		}
		boolean equals(M2 other) {
			if (other._w != _w) { return false; }
			return java.util.Arrays.equals(_data, other._data);
		}
		public boolean get(int f,int e)
		{
			return _data[_w*e + f];
		}
		public void set(int f, int e)
		{
			//System.out.println("Set("+x+", "+y+")");
			try {
			 _data[_w*e + f] = true;
			} catch (ArrayIndexOutOfBoundsException ee) {
				throw new RuntimeException("Set(" + f + ", " + e + "): caught " + ee);
			}
		}
		public void reset(int f, int e)
		{
			_data[_w*e + f] = false;
		}
		public void readFields(DataInput in) throws IOException {
			_w = in.readShort();
			int size = in.readChar();
			if (size < 1)
				throw new RuntimeException("Error: " + size + " is not good for alignment!");
			_data = new boolean[size];
			int bbLen = in.readInt();
			short[] faps = new short[bbLen/2];
			short[] eaps = new short[bbLen/2];
			ByteBuffer bb=ByteBuffer.allocate(bbLen);
			in.readFully(bb.array());
			ShortBuffer sb = bb.asShortBuffer();
			sb.get(faps);
			bb.clear();
			in.readFully(bb.array());
			sb = bb.asShortBuffer();
			sb.get(eaps);
			for (int i = 0; i<faps.length; i++) {
				set(faps[i], eaps[i]);
			}
		}
		public void write(DataOutput out) throws IOException {
			out.writeShort(_w);
			out.writeShort((short)_data.length);
			int c = 0;
			for (int i=0; i< _data.length; i++)
				if (_data[i]) c++;
			short[] faps = new short[c];
			short[] eaps = new short[c];
			c = 0;
			for (int i=0; i< _data.length; i++)
				if (_data[i]) {
					faps[c] = (short)(i % _w);
					eaps[c] = (short)(i / _w);
					c++;
				}
			int bbLen = faps.length * 2;
			out.writeInt(bbLen);
			ByteBuffer bb=ByteBuffer.allocate(bbLen);
			ShortBuffer sb = bb.asShortBuffer();
			sb.put(faps);
			out.write(bb.array());
			sb.clear();
			sb.put(eaps);
			out.write(bb.array());
		}
	}
	protected short _elen;
	protected short _flen;
	boolean[] faligned;
	boolean[] ealigned;
	M2 _aligned;
	static Pattern eline_re = Pattern.compile("([^\\s]+)\\s+\\(\\{\\s+((?:\\d+\\s+)*)\\}\\)");
	public static final int[][] DIAG_NEIGHBORS = //{{-1,-1},{0,-1},{1,-1},{-1,0},{1,0},{-1,1},{0,1},{1,1}};
	                                      {{0,-1},{-1,0},{1,0},{0,1},{1,1},{-1,-1},{1,-1},{-1,1}};
	public static final int[][] NEIGHBORS = {{0,-1},{-1,0},{1,0},{0,1}};
	public static final int[][][] LNEIGHBORS =  {{{ 0,+1},{+1,+1},{+1, 0}},
	                                      {{+1, 0},{+1,-1},{ 0,-1}},
	                                      {{ 0,-1},{-1,-1},{-1, 0}},
	                                      {{-1, 0},{-1,+1},{ 0,+1}}};

	public int countAlignmentPoints() {
		int count = 0;
		for (int j=0; j < _flen; j++)
			for (int i = 0; i < _elen; i++)
				if (this.aligned(j, i))
					count += 1;
		return count;
	}

	public Alignment mergeEnglishWords(int i, int j) {
		if (i+1 != j)
			throw new IllegalArgumentException("mergeEnglishWords can only combine adjacent positions! " + i + "," + j);
		Alignment res = new Alignment(_flen, _elen - 1);
		for (int a = 0; a < _flen; a++)
			for (int b = 0; b < _elen; b++)
				if (this.aligned(a, b)) {
					int ee = b;
					if (b > i) ee--;
					res.align(a, ee);
				}
		return res;
	}
	public Alignment splitEnglishWords(int i) {
		Alignment res = new Alignment(_flen, _elen + 1);
		for (int a = 0; a < _flen; a++)
			for (int b = 0; b < _elen; b++)
				if (this.aligned(a, b)) {
					int ee = b;
					if (b == i)
						res.align(a, i);
					if (b >= i)
						ee++;
					res.align(a, ee);
				}
		return res;
	}	
	public Alignment splitForeignWords(int j) {
		Alignment res = new Alignment(_flen + 1, _elen);
		for (int a = 0; a < _flen; a++)
			for (int b = 0; b < _elen; b++)
				if (this.aligned(a, b)) {
					int ee = a;
					if (a == j)
						res.align(j, b);
					if (a >= j)
						ee++;
					res.align(ee, b);
				}
		return res;
	}	
	public void readFields(DataInput in) throws IOException {
		if (_aligned == null)
			_aligned = new M2();
		_aligned.readFields(in);
		_flen = _aligned._w;
		_elen = (short)(_aligned._data.length / _flen);
		faligned = new boolean[_flen];
		ealigned = new boolean[_elen];
		for (int f=0; f<_flen; f++)
			for (int e=0; e<_elen; e++)
				if (aligned(f,e)) {
					faligned[f]=true;
					ealigned[e]=true;
				}
		
	}
	
	public byte getType() {
		return 1;
	}
	
	public Object clone() {
		Alignment res = new Alignment();
		res._aligned = (M2)_aligned.clone();
		res._elen = _elen;
		res._flen = _flen;
		res.ealigned = ealigned.clone();
		res.faligned = faligned.clone();
		return res;
	}

	public void write(DataOutput out) throws IOException {
		_aligned.write(out);
	}

	public boolean equals(Object o) {
		if (!(o instanceof Alignment)) { return false; }
		return _aligned.equals(((Alignment)o)._aligned);
	}
	public boolean neighborAligned(int i, int j)
	{
		return countNeighbors(i, j, DIAG_NEIGHBORS) > 0;
	}
	public boolean lneighborAligned(int i, int j)
	{
		for (int x=0;x<LNEIGHBORS.length;x++) {
			if (countNeighbors(i, j, LNEIGHBORS[x]) >= 2)
				return true;
		}
		return false;
	}
	
	public java.util.Iterator<Alignment.IntPair> iterator() {
		return new AIterator(this);
	}

	public final int countNeighbors(int f, int e, int[][] rels)
	{
		int res = 0;
		for (int x=0; x<rels.length; x++) {
			int cf = f + rels[x][0];
			int ce = e + rels[x][1];
			if (cf >= 0 && cf < _flen && 
				ce >= 0 && ce < _elen && aligned(cf, ce)) {
				res++; }
		} 
		return res;
	}
	public final boolean rookAligned(int i, int j)
	{
		return faligned[i] || ealigned[j]; 
	}
	public final boolean doubleRookAligned(int i, int j)
	{
		return faligned[i] && ealigned[j]; 
	}
	public final int getELength()
	{
		return _elen;
	}
	public final int getFLength()
	{
		return _flen;
	}
	public Alignment()
	{
		_elen = 0;
		_flen = 0;
		_aligned = null;
	}
	public Alignment(int flen, int elen)
	{		
		_elen = (short)(elen);
		_flen = (short)(flen);
		alloc();
	}
	public Alignment(int flen, int elen, String pa) {
		_elen = (short)elen;
		_flen = (short)flen;
		alloc();
		if (pa == null || pa.length() == 0) return;
		String[] aps = pa.split("\\s+");
		for (String ap : aps) {
			String[] pair = ap.split("-");
			if (pair.length != 2)
				throw new IllegalArgumentException("Malformed alignment string: " + pa);
			int f = Integer.parseInt(pair[0]);
			int e = Integer.parseInt(pair[1]);
			if (f >= _flen || e >= _elen)
				throw new IndexOutOfBoundsException("out of bounds: " + f + "," + e);
			align(f, e);
		}
	}
	private void alloc()
	{
		faligned = new boolean[_flen];
		ealigned = new boolean[_elen];
		_aligned = new M2(_flen,_elen);
	}
	public final boolean aligned(int f, int e)
	{
		return _aligned.get(f,e);
	}
	public final void align(int f, int e)
	{
		_aligned.set(f,e);
		faligned[f] = true;
		ealigned[e] = true;
	}
	public final boolean isEAligned(int e) {
		return ealigned[e];
	}
	public final boolean isFAligned(int f) {
		return faligned[f];
	}
	
	public final void unalignF(int f) {
		faligned[f] = false;
		for (int i=0; i<_elen; i++)
			_aligned.reset(f, i);
	}

	public final void unalignE(int e) {
		ealigned[e] = false;
		for (int i=0; i<_flen; i++)
			_aligned.reset(i, e);
	}

	public static Alignment fromGiza(String eline, String fline, boolean transpose) {
		Matcher es = eline_re.matcher(fline);
		es.find();
		boolean skipNull = false;
		if (es.group(1).equals("NULL")) {
			skipNull = true;
		} else {
			es.reset();
		}
		ArrayList<String> afwords = new ArrayList<String>();
		while (es.find()) {
//			System.out.format("Str: %s  aligns: '%s'\n", es.group(1), es.group(2));
			afwords.add(es.group(1));
		}
		String[] ewords = eline.split("\\s+");
		Alignment al = null;
		if (transpose) {
			al = new Alignment(ewords.length, afwords.size());
		} else {
			al = new Alignment(afwords.size(), ewords.length);
		}
		es.reset();
		if (skipNull) { es.find(); }
		int i = 0;
		while (es.find()) {
			String saligns = es.group(2);
			if (!saligns.matches("^\\s*$")) {
				String[] aligns = saligns.split("\\s+");
				for (int k=0; k<aligns.length; k++)
				{
					int j = Integer.parseInt(aligns[k]) - 1;
					if (transpose)
						al.align(j, i);
					else
						al.align(i, j);
				}
			}
			i++;
		}
		return al;
	}
	public Alignment getTranspose() {
		Alignment res = new Alignment(_elen, _flen);
		for (int ei=0; ei<_elen; ei++)
			for (int fi=0; fi<_flen; fi++)
				if (aligned(fi, ei)) res.align(ei, fi);
		return res;
	}
	public String toStringVisual() {
		StringBuffer sb = new StringBuffer();
		sb.append(' ');
		for (int j=0; j<_flen; j++)
			sb.append(j % 10);
		sb.append('\n');
		for (int i=0; i<_elen; i++) {
			sb.append(i % 10);
			for (int j=0; j<_flen; j++) {
				if (aligned(j,i))
					sb.append('*');
				else
					sb.append('.');
			}
			sb.append('\n');
		}
		return sb.toString();
	}
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		for (int i=0; i<_flen; i++)
			for (int j=0; j<_elen; j++)
				if (aligned(i, j))
					sb.append(i).append('-').append(j).append(' ');
		if (sb.length() > 0)
			sb.delete(sb.length()-1, sb.length());
		return sb.toString();
	}
	
	public static Alignment intersect(Alignment a1, Alignment a2)
	{
		Alignment a = new Alignment(a1._flen, a1._elen);
		for (int i=0; i<a1._flen; i++)
			for (int j=0; j<a1._elen; j++)
				if (a1.aligned(i, j) &&
					a2.aligned(i, j))
					a.align(i,j);
		return a;
	}
	
	public static Alignment union(Alignment a1, Alignment a2)
	{
		Alignment a = new Alignment(a1._flen, a1._elen);
		for (int i=0; i<a1._flen; i++)
			for (int j=0; j<a1._elen; j++)
				if (a1.aligned(i, j) ||
					a2.aligned(i, j))
					a.align(i,j);
		return a;
	}	

}
